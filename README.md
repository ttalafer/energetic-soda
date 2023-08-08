# ENERGETIC SODA PIPELINE
ExtendHealth Coding challenge
---
## TECH
Solution was implemented the following technologies:

- Python 3.11.2 and the libraries included in requirements.txt.
- PostgreSQL 15

## DATA MODEL
In order to create the model, run the contents of __/SQL/energetic_soda_pg.sql__ in a PostgreSQL instance after creating a db called energetic_soda and connecting to it.

---
## USAGE
In order to use the pipeline the following commands should be used:
- run_load() will process the files located in the __/Files/__ folder. to use it, execute the following:

    `python energetic-soda.py run_load` 

- get_season_data(season) will produce the input season score summary, print it in the terminal and produce the output as a csv file with the season name under __/Files/__ folder. To use it run:
    `python energetic-soda.py get_season_data 2010-11` 

## OVERVIEW

Model consists of 2 schemas, doc (short for Documents) and cds (Central Data Storage), the former behaving like a datalake, storing the documents on a per-row basis, for easier re-processing if needed. The latter schema, stores the documents prepared for analysis, allowing for a faster and 


Python solution has been implemented with flexibility in mind - meaning, it can be easily re-directed to use MSSQL instead of PostgreSQL, just needeing the correct connection to the MSSQL instance and the model being properly translated to that engine's syntax.
 
 It has 2 main functions:
  -  `run_load()`: in charge of loading all files into doc.lake table, with each record in JSON format, as well as triggering the process that converts that into more readable data for easier and faster analysis. Process
  - `get_season_data(season:str)`: dedicated to the generation of the general view of the season sent as parameter, via store procedure in the database, printing the results in the console and dumping it into a csv file with the season name. If no season is provided, a message indicating this will be shown in the console and no file will be generated

## ANALISYS
Analisys queries and conclusion can be found under the __/Analysis/__ folder
