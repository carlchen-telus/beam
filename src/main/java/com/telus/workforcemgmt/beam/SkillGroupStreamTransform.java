package com.telus.workforcemgmt.beam;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

public class SkillGroupStreamTransform extends PTransform<PCollection<String>, PCollection<String>> {

	
	@Override
	public PCollection<String> expand(PCollection<String> lines) {

		final PCollectionView<List<String>> technicianList = lines.apply("skillGroupStream", JdbcIO
				.<String, String>readAll().withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create("org.postgresql.ds.PGPoolingDataSource", "jdbc:postgresql://localhost:5432/ngcm")
				          .withUsername("wfm-dbuser_dv")
				          .withPassword("wfm-dbuser_dv"))
				.withQuery(
						"SELECT DEMAND_STREAM_SUPPLY_SKILL.TEAM_MEMBER_ID, DEMAND_STREAM_SUPPLY.EFFECTIVE_DT, STRING_AGG (SKILL_CD,'|' ORDER BY SKILL_CD) SKILLS FROM DEMAND_STREAM_SUPPLY_SKILL, DEMAND_STREAM_SUPPLY WHERE DEMAND_STREAM_SUPPLY_SKILL.TEAM_MEMBER_ID = DEMAND_STREAM_SUPPLY.TEAM_MEMBER_ID AND DEMAND_STREAM_SUPPLY_SKILL.EFFECTIVE_DT = DEMAND_STREAM_SUPPLY.EFFECTIVE_DT AND DEMAND_STREAM_SUPPLY.DEMAND_STREAM_ID = ? AND DEMAND_STREAM_SUPPLY.EFFECTIVE_DT = ? AND TO_CHAR(DEMAND_STREAM_SUPPLY.EFFECTIVE_END_TS,'YYYY-MM-DD') = '9999-12-31' AND TO_CHAR(DEMAND_STREAM_SUPPLY_SKILL.EFFECTIVE_END_TS,'YYYY-MM-DD') = '9999-12-31' group by DEMAND_STREAM_SUPPLY_SKILL.TEAM_MEMBER_ID")
				.withParameterSetter(new JdbcIO.PreparedStatementSetter<String>() {
					@Override
					public void setParameters(String element, PreparedStatement preparedStatement) throws Exception {
						String[] value = element.split(",");
						preparedStatement.setLong(1, Long.parseLong(value[0]));
						preparedStatement.setDate(2, Date.valueOf(value[1]));
					}
				})
				.withCoder(StringUtf8Coder.of())
				.withRowMapper(new JdbcIO.RowMapper<String>() {
					public String mapRow(ResultSet resultSet) throws Exception {
						String techId = resultSet.getString("TEAM_MEMBER_ID");
						String skillCds = resultSet.getString("SKILLS");
						Date effDate = resultSet.getDate("EFFECTIVE_DT");
						return String.format("%s,%s,%s", techId, effDate.toString(), skillCds);
					}
				})).apply(View.asList());

		PCollection<String> skillGroupStream = lines.apply("skillGroupStream", JdbcIO.<String, String>readAll()
				.withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create("org.postgresql.ds.PGPoolingDataSource", "jdbc:postgresql://localhost:5432/ngcm")
				          .withUsername("wfm-dbuser_dv")
				          .withPassword("wfm-dbuser_dv"))
				.withQuery(
						"select DEMAND_STREAM_ID, SKILL_GROUP_STREAM.SKILL_GROUP_ID, SKILL_GROUP_STREAM_ID, SKILLS from SKILL_GROUP_STREAM, "
								+ " (select skill_group_id, STRING_AGG (SKILL_GROUP_MAPPING.SKILL_TYPE_CD,'|' ORDER BY SKILL_GROUP_MAPPING.SKILL_TYPE_CD) SKILLS "
								+ "	from SKILL_GROUP_MAPPING where CURRENT_TIMESTAMP between SKILL_GROUP_MAPPING.EFFECTIVE_START_TS AND SKILL_GROUP_MAPPING.EFFECTIVE_END_TS "
								+ " GROUP by skill_group_id) sks "
								+ " WHERE sks.SKILL_GROUP_ID = SKILL_GROUP_STREAM.SKILL_GROUP_ID "
								+ " AND CURRENT_TIMESTAMP between SKILL_GROUP_STREAM.EFFECTIVE_START_TS AND SKILL_GROUP_STREAM.EFFECTIVE_END_TS"
								+ " AND SKILL_GROUP_STREAM.DEMAND_STREAM_ID = ? ")
				.withParameterSetter(new JdbcIO.PreparedStatementSetter<String>() {
					public void setParameters(String element, PreparedStatement preparedStatement) throws Exception {
						String[] value = element.split(",");
						preparedStatement.setLong(1, Long.parseLong(value[0]));
					}
				})
				.withCoder(StringUtf8Coder.of())
				.withRowMapper(new JdbcIO.RowMapper<String>() {
					public String mapRow(ResultSet resultSet) throws Exception {
						Long demandStreamId = resultSet.getLong("DEMAND_STREAM_ID");
						Long skillGroupId = resultSet.getLong("SKILL_GROUP_ID");
						Long skillGroupStreamId = resultSet.getLong("SKILL_GROUP_STREAM_ID");
						String skillCds = resultSet.getString("SKILLS");
						return String.format("%s,%s,%s,%s", demandStreamId, skillGroupId, skillGroupStreamId,skillCds);
					}
				}));

		PCollection<String> resultingPCollection = skillGroupStream.apply(ParDo.of(new DoFn<String, String>() {
			@ProcessElement
			public void processElement(ProcessContext c) {
				List<String> technicians = c.sideInput(technicianList);
				String skillGroupStream = c.element();
				String[] skillsReq = getSkillGroupSkills(skillGroupStream);
				technicians.stream().forEach(tech -> {
					String[] techSkills = getTechnicianSkills(tech);
					if (Arrays.asList(techSkills).containsAll(Arrays.asList(skillsReq))) {
						c.output(new StringBuilder(skillGroupStream)
								.append(",").append(getTechnicianId(tech))
								.append(",").append(getEffectiveDate(tech))
								.toString()); //demandStreamId, skillGroupId, skillGroupStreamId,skillCds,techId, effectiveDate
					}
				});
			}
		}).withSideInputs(technicianList));
		return resultingPCollection;
	}

	public static String getTechnicianId(String line) {
		String[] skgd = line.split(",");
		return skgd[0];
	}

	public static String[] getTechnicianSkills(String line) {
		String[] skgd = line.split(",");
		return skgd[2].split("|");
	}

	public static String getEffectiveDate(String line) {
		String[] skgd = line.split(",");
		return skgd[1];
	}
	
	public static String[] getSkillGroupSkills(String line) {
		String[] skgd = line.split(",");
		return skgd[3].split("|");
	}

	public static boolean linearIn(Integer[] outer, Integer[] inner) {
		return Arrays.asList(outer).containsAll(Arrays.asList(inner));
	}
}
