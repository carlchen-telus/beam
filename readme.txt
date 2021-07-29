1. load and run pipeline in google cloud console:
 > git clone https://github.com/carlchen-telus/beam.git
 > cd beam
$beam>mvn compile exec:java -Dexec.mainClass=com.telus.workforcemgmt.beam.DemandSkillGroupAssignmentProcessor

2. Error:
 Causes: Subnetwork https://www.googleapis.com/compute/v1/projects/cio-wfm-messaging-lab-f81efa/regions/us-east1/subnetworks/wfm-vpc-11d74588 is not accessible to Dataflow Service account or does not exist
 Solution: use Google Command Line to check region and subnet (subnetwork):
 > glcoud compute networks subnets list 
NAME                 REGION       NETWORK           RANGE
wfm-subnet-11d74588  us-central1  wfm-vpc-11d74588  10.10.0.0/24
 
3. Error: Startup of the worker pool in zone us-central1-f failed to bring up any of the desired 1 workers. The project quota may have been exceeded or access control policies may be preventing the operation; review the Stackdriver Logging "GCE VM Instance" log for diagnostics.
 Solution:
Follow instruction in https://cloud.google.com/dataflow/docs/guides/common-errors#worker-pool-failure, found log:
Constraint constraints/compute.vmExternalIpAccess violated for project 58731396595. Add instance projects/cio-wfm-messaging-lab-f81efa/zones/us-central1-c/instances/demandskillgroupassignmen-06181629-e6b9-harness-hcqr to the constraint to use external IP with it. 
go to Home > IAM & Admin > Identity & Organization > Define proper permissions.
found "Define allowed external IPs for VM instances" is external IPs are disabled.

https://cloud.google.com/dataflow/docs/guides/routes-firewall#firewall_rules_required_by
As per above instruction, create a firewall rule with tag "dataflow"

gcloud compute firewall-rules create dataflow \
    --network wfm-vpc-11d74588 \
    --action allow \
    --direction ingress \
    --target-tags dataflow \
    --source-tags dataflow \
    --priority 0 \
    --rules tcp:12345-12346
	
 By default Dataflow always adds the network tag dataflow to every worker VM it creates (https://cloud.google.com/dataflow/docs/guides/routes-firewall#enabling_network_tags)
 
 
 
 

 
 
 
 
