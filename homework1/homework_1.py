"""
Module 1 Homework

Docker & SQL
In this homework we'll prepare the environment and practice with Docker and SQL

Question 1. Knowing docker tags
Run the command to get information on Docker

docker --help

Now run the command to get help on the "docker build" command:

docker build --help

Do the same for "docker run".

Which tag has the following text? - Automatically remove the container when it exits

--delete
--rc
--rmc
--rm
"""
# ANSWER: 
# using the docker run --help, we get answer from the terminal. In the appendix -q we can read 
# the correct answer is (--rm)

"""
Question 2. Understanding docker first run
Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash. Now check the python modules that are installed ( use pip list ).

What is version of the package wheel ?

0.42.0
1.0.0
23.0.1
58.1.0
"""
# ANSWER
# using the command "docker run -it --entrypoint=//bin/bash python:3.9". The double // tells git not to conver the path for windows.
# and then "pip list", so we get wheel 0.42.0

"""
Prepare Postgres
Run Postgres and load data as shown in the videos We'll use the green taxi trips from September 2019:

wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz

You will also need the dataset with zones:

wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv

Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)

Question 3. Count records
How many taxi trips were totally made on September 18th 2019?

Tip: started and finished on 2019-09-18.

Remember that lpep_pickup_datetime and lpep_dropoff_datetime columns are in the format timestamp (date and hour+min+sec) and not in date.

15767
15612
15859
89009
"""
# To answer this question we upload both the data and data files to out local postgres database using the snnipets:
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz"

winpty docker run -it \
  --network=pg-network \
  upload_green:v1.0 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=ny_taxi \
    --table_name=green_taxi_trips \
    --url=${URL}

# and
URL="https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"

winpty docker run -it \
  --network=pg-network \
  taxi_zones:v01 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=ny_taxi \
    --table_name=zones_table \
    --url=${URL}


# The we can count the trips into pg-admin using the following postgresql snippet:
select count(1) from green_taxi_trips
where date(lpep_pickup_datetime)='2019-09-18'
and date(lpep_dropoff_datetime)='2019-09-18'

# ANSWER
# hence, we get an amount of 15612 trips.

"""
Question 4. Largest trip for each day
Which was the pick up day with the largest trip distance Use the pick up time for your calculations.

2019-09-18
2019-09-16
2019-09-26
2019-09-21
"""
# we write the code as follows:
select 
	date(lpep_pickup_datetime) as pick_up_day,
	max(trip_distance) as biggest_distance
from green_taxi_trips
group by date(lpep_pickup_datetime)
order by biggest_distance desc 
limit 3

# ANSWER 
# The largest trip during septembre 2019 was performed in 2019-09-26

"""
Question 5. Three biggest pick up Boroughs
Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown

Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?

"Brooklyn" "Manhattan" "Queens"
"Bronx" "Brooklyn" "Manhattan"
"Bronx" "Manhattan" "Queens"
"Brooklyn" "Queens" "Staten Island"

"""
# Using the following code

select 
	public.zones_table."Borough",
	sum(public.green_taxi_trips.total_amount) as sum_amount
from 
	public.zones_table 
left join public.green_taxi_trips
on public.zones_table."LocationID" = public.green_taxi_trips."PULocationID"
where
	date(public.green_taxi_trips.lpep_pickup_datetime)='2019-09-18'
group by 
	public.zones_table."Borough"
order by 
	sum_amount desc

# ANSWER
# Running the snippet above we identify that the 3 pick up Boroughs that had a sum of total_amount superior to 50000 are "Brooklyn" "Manhattan" "Queens" 

"""
Question 6. Largest tip
For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip? We want the name of the zone, not the id.

Note: it's not a typo, it's tip , not trip

Central Park
Jamaica
JFK Airport
Long Island City/Queens Plaza
"""

# Using the following code 

select 
	z2."Zone",
	max(public.green_taxi_trips.tip_amount) as tips
from 
	public.green_taxi_trips
left join public.zones_table z
on z."LocationID" = public.green_taxi_trips."PULocationID"
inner join public.zones_table z2
on z2."LocationID" = public.green_taxi_trips."DOLocationID"
where 
	z."Zone" = 'Astoria'
group by 
	z2."Zone"
order by 
	tips desc
limit 3

# ANSWER
# We obtain that "JFK Airport" was the drop off zone that had the largest tip.

"""
Terraform
In this section homework we'll prepare the environment by creating resources in GCP with Terraform.

In your VM on GCP/Laptop/GitHub Codespace install Terraform. Copy the files from the course repo here to your VM/Laptop/GitHub Codespace.

Modify the files as necessary to create a GCP Bucket and Big Query Dataset.

Question 7. Creating Resources
After updating the main.tf and variable.tf files run:

terraform apply
Paste the output of this command into the homework submission form.
"""

"""
Plan: 2 to add, 0 to change, 0 to destroy.

Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value: yes

google_bigquery_dataset.demo_dataset: Creating...
google_storage_bucket.demo-bucket: Creating...
google_bigquery_dataset.demo_dataset: Creation complete after 1s [id=projects/data-taxi-1/datasets/demo_dataset]
google_storage_bucket.demo-bucket: Creation complete after 1s [id=terraform-taxi-data-1]

Apply complete! Resources: 2 added, 0 changed, 0 destroyed.
"""