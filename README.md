# Database_and_Analytics_Project_Semester_1
This is a project of "Database and Analytics Programming" which I completed during Semester 1 of MSc. Data Science course.

We have used Dagster opensource framework for the implementation of pipeline in our project. 
We are using Amazon’s AWS S3 Cloud Storage to store all 3 datasets, so we do not have to access local .csv files to push to our databases. 
We will directly pull data from AWS S3 Cloud Storage and create DataFrame using Pandas and push all data to MongoDB Cloud Database using Python Code. 
Afterwards, we will use Pandas to pull all data from MongoDB Cloud Database and create another DataFrame using Pandas. 
Now, we will convert and push this DataFrame into AWS RDS MySQL Database using Python. 
Again, we will access AWS RDS MySQL Database and pull the data into DataFrames using Pandas.
We will use these 3 DataFrames for further Visualization for all 3 datasets separately. 

After Visualization, we will combine few variables of all 3 datasets, to show relations of few Pandemic Scenarios. 
After combing all 3 DataFrames important variables into 1 DataFrame we will push this DataFrame to IBM Watson Cloud Database. 
Afterwards, we will access IBM Watson Cloud Database and pull the data using Python by creating a final DataFrame for final Visualization which shows overall information.
