#Job Emulator
This module is responsible for emulation of workloads  from specific datasets and especially 
for the  Intelligent  Transportation and  Vehicular  networks (Dublin Smart City Buses Network, NYC Taxis).

To execute the emulator, you should specify the number of vehicles (`job_num`), 
the sampling period in milliseconds (`job_period`) and the type of emulation (`job_type`). 

`JobEmulator <job_num> <job_period> <job_type> [<job_path>]`

In the moment, we have already implemented a type for Dublin Smart City Buses Network (`BusJob`) 
and a general csv reader emulator (`GeneralCsvToJSONJob`). 
The general emulator reads the first line of the csv, which considers it as the labels of the data,
and generates a json object for every next line.

If you wish to implement an external job emulator class, you have to extent the `Job` class 
and pass the `job_path` as parameter to the script.