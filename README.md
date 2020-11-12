# Brief  
The code in src/ does the following things:
1. Task 1: From the data filter "Cessna" manufactured planes, and show the top 3 models most made models (highest number of planes for the models).
2. Show the min, max, avg and the amount of delay (in minutes) for each airline going to the US or as defined in main params.
3. Find all the "Cessna" made flights in the US or as defined in the parameters and show the top 5 most used models.

# Job Design Documentation:

## Task 1: 
1. Filter planes manufactured by Cessna
2. Join flights with tail number to get all the flights
3. Group reduce: Concatenates Cessna Model number and reduces it to 3 digits	
4. Sort partition: group each model, count how many for each model and then sort. The output will contain the top 3 models
5. Group Reduce again
6. Data sink: Finally, add the model to the Manufacture string and shorten string to 10 digits for output

## Task 2:
1. Select carrier code, airline name from airline; carrier code, scheduled arrival, actual arrival from flights.
2. Filter flights to airline destination to US.
3. Find delay in minutes from arrival and departure time for each tuple for flights 
4. Filter cases where there is no actual arrival (cancelled flights).
convert string to time and then time to Long to find the diff in long. This is the delay in milliseconds, convert this to seconds.
5. Filter the flights dataset where the delay is <1 (as the delay is less than 1 minute, there is no delay).
6. Join the new flights dataset and airline dataset.
7. Convert it to table and find the min, max, avg and count of delay using sql.
8. Write to sink.


## Task 3: 
1. Select the Airport code and country
2. Filter by only the airports in the country
3. Select flight ID, flight origin and tail number and join with the Airport code
4. Select tail number, manufacturer, model number and year
5. Concatenate Cessna model number and reduce it down to 3 digits
6. Join flights with tail number to collect all the flights
7. Count the number of airline and model and sort on airline, model.
8. Broadcast this dataset and iterate through another (RichMap) dataset with same values to manually compare the tuples in a dataset.
9. Iterate through each tuple in the data set; for each tuple check if the airline name in the outer tuple matches to any airline name in the inner tuple. If it does match, select the matching model number and store it in the result tuple
10. This creates duplicates as it creates a list of models for each   
           <Aircraft, model> tuple in the dataset, so filter out distinct tuples.
11. Write to sink.


## Justification of Tuning Decisions or Optimisations:
The most frequent optimization performed is Filtering before Joining. This is a good practice to adhere to especially when analyzing increasing/decreasing data sizes as It increases the efficiency of the query optimizer.

### Task 1: 
In our unoptimised solution, we did not use any join optimisations and had our filter after the join. In order to optimise the solution we moved the filter before the join therefor the join will have fewer rows to work with as some will have been filtered out. Additionally, there is a small number of aircraft so we have used the ‘joinWithTiny’ join optimisation. 

### Task 2:
In the unoptimised solution we again did not filter the country codes first or remove cancelled flights first. We set the delay for cancelled flights as -1(minute) and proceeded to filter all the delays <1 minute towards the end after joining. However, in the optimised solution, the filters were at appropriate places that reduced the size of the dataset to operate on.
Also, we gave flink certain map hints (read fields and forwarded fields) for each map to help optimise it further.

### Task 3:
Like the previous two tasks, we had the filter after the join. Moving the filter before the join allows a reduced number of rows to be processed when joined. Again we used the ‘joinWithTiny’ join optimisation in order to optimise this join.

Tuning Decision:
Filtering before joins: Helps as we perform projection before join to remove unnecessary computation.
Read Fields: Gives flink hints about fields in the tuples that the map function will process.
Forwarded Fields: Gives hints to flink about the fields of a tuple that are directly returned.








