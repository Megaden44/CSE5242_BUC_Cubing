# CSE5242_BUC_Cubing
BUC Cubing algorithm performance analysis

# Run the program
1. After downloading the python script run the main function
2. You will be prompted to enter Data Size, Filter level, and Verbose
3. Results are printed to the console, if you enable verbose you can see what the program is doing

# View the Database validation
Copy "data-cubing.db" into DataGrip

To verify to cubing results of the algorithms run the following script in DataGrip:

select avg(aggregate_column) from cubing_data

select avg(aggregate_column) from cubing_data group by A

select avg(aggregate_column) from cubing_data group by A, B

select avg(aggregate_column) from cubing_data group by A, C

select avg(aggregate_column) from cubing_data group by A, D

select avg(aggregate_column) from cubing_data group by A, E

select avg(aggregate_column) from cubing_data group by A, B, C

select avg(aggregate_column) from cubing_data group by A, B, C, D

select avg(aggregate_column) from cubing_data group by A, B, C, D, E

select avg(aggregate_column) from cubing_data group by A, C, D

select avg(aggregate_column) from cubing_data group by A, C, D, E

select avg(aggregate_column) from cubing_data group by A, D, E

select avg(aggregate_column) from cubing_data group by A, B, D

select avg(aggregate_column) from cubing_data group by A, B, E

select avg(aggregate_column) from cubing_data group by A, C, E

select avg(aggregate_column) from cubing_data group by B

select avg(aggregate_column) from cubing_data group by B, C

select avg(aggregate_column) from cubing_data group by B, D

select avg(aggregate_column) from cubing_data group by B, E

select avg(aggregate_column) from cubing_data group by B, C, D

select avg(aggregate_column) from cubing_data group by B, C, E

select avg(aggregate_column) from cubing_data group by B, C, D, E

select avg(aggregate_column) from cubing_data group by C

select avg(aggregate_column) from cubing_data group by C, D

select avg(aggregate_column) from cubing_data group by C, E

select avg(aggregate_column) from cubing_data group by C, D, E

select avg(aggregate_column) from cubing_data group by D

select avg(aggregate_column) from cubing_data group by D, E

select avg(aggregate_column) from cubing_data group by E

