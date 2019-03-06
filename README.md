# telenor_etl

telenor_etl is a **python** project. It performs an ETL (Extract Transform Load) process to process 
data. In this process data is fetched from one database, transformed into required format and 
loaded to a destinatin database as necessary queries can be executed from that destination database.

# Installation
Install python in your machine(I used ubuntu os which contains python by default)

Then go to your terminal and clone this project from git using the command below:

    git clone https://github.com/jahangir091/telenor_etl.git

Then go to the directory **telenor_etl/etl** and run command:

    pip install -r requirements.txt

This command will install necessary python libraries.

# Run ETL

Now type **python** and hit enter. This command will take you to python console and then run etl as below.

    from extract import Extract
  
    etl = Extract()
  
    etl.etl()

This will first connect to the source database which is added to the **database.ini** file
then will load data from the source database and finally will call **load** method of **Load** class which is in **load.py** file.
