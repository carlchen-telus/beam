package com.telus.workforcemgmt.beam;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class DemandSkillGroupAssignmentProcessor {

	final static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-m-d");


	public static void main(String[] args) {
		final Long batchInstanceLogId = 0L;
		PipelineOptions options = PipelineOptionsFactory.create();
		DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
		dataflowOptions.setRunner(DataflowRunner.class);
		dataflowOptions.setRegion("us-east1");
		dataflowOptions.setProject("cio-wfm-messaging-lab-f81efa");
		dataflowOptions.setSubnetwork("regions/us-central1/subnetworks/wfm-subnet-11d74588");
		dataflowOptions.setTempLocation("gs://dataflow_ngcm/temp");
		Pipeline p = Pipeline.create(options);
		PCollection<String> demandStreamIds =
				p.apply("get demand stream id ", 
						JdbcIO.<String>read()
						.withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create("org.postgresql.ds.PGPoolingDataSource", "jdbc:postgresql://localhost:5432/ngcm")
						          .withUsername("wfm-dbuser_dv")
						          .withPassword("wfm-dbuser_dv"))
						.withQuery("select distinct demand_stream_id from SKILL_GROUP_STREAM  WHERE CURRENT_TIMESTAMP between SKILL_GROUP_STREAM.EFFECTIVE_START_TS AND SKILL_GROUP_STREAM.EFFECTIVE_END_TS")
						.withRowMapper(new JdbcIO.RowMapper<String>() {
							public String mapRow(ResultSet resultSet) throws Exception {
								return resultSet.getString(1);
							}
						})
						.withCoder(StringUtf8Coder.of()))
				.apply("add effective date", ParDo.of(new DoFn<String,String>() {
					@ProcessElement
					public void processElement(ProcessContext c) {
						String demandStreamId = (String) c.element();
						LocalDate now = LocalDate.now();
						for (int i = 0; i < 84; i++) {
							LocalDate effDate = now.plusDays(i);
							c.output(new StringBuilder(demandStreamId).append(",").append(effDate.toString()).toString());		                    	
						}
					}}));
		demandStreamIds.apply(new SkillGroupStreamTransform())
		.apply(JdbcIO.<String>write()
				.withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create("org.postgresql.ds.PGPoolingDataSource", "jdbc:postgresql://localhost:5432/ngcm")
				          .withUsername("wfm-dbuser_dv")
				          .withPassword("wfm-dbuser_dv"))
				.withStatement("INSERT INTO DS_SKILL_GROUP_ASSIGNMENT "
						+ "	(DS_SKILL_GROUP_ASSIGNMENT_ID, BATCH_JOB_INSTANCE_LOG_ID, EFFECTIVE_DT, TEAM_MEMBER_ID, DEMAND_STREAM_ID, "
						+ "		SKILL_GROUP_STREAM_ID, SKILL_GROUP_ID, EFFECTIVE_START_TS, EFFECTIVE_END_TS, CREATE_USER_ID, "
						+ "     CREATE_TS, LAST_UPDT_USER_ID, LAST_UPDT_TS) "
						+ "		VALUES (NEXTVAL('DS_SKILL_GROUP_ASSIGNMENT_SEQ'), ?, ?, ?, ?, ?, ? "
						+ "    	CURRENT_TIMESTAMP, TO_TIMESTAMP('9999-12-31 00:00:00','YYYY-MM-DD HH24:MI:SS'), 'PACE_BATCH',"
						+ "    	CURRENT_TIMESTAMP, 'PACE_BATCH', SYSTIMESTAMP)")
				.withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<String>() {
					public void setParameters(String element, PreparedStatement query) throws SQLException {
						String[] value = element.split(",");
						query.setLong(1, batchInstanceLogId); //batchInstanceLogId
						query.setString(2, value[5]); //effective date
						query.setString(3, value[4]); //technician id
						query.setString(4, value[0]); //demandstreamid
						query.setString(5, value[2]); //skillgroupstreamId
						query.setString(6, value[1]); //skillgroupId
					}
				})
				);
	    p.run().waitUntilFinish();
	}

}

