# DE-Project

1-Overview of the used dataset.

  The used dataset contains all the information about everyone who participated in the olympics.
  Each row represents an athlete with the following attributes:
	
          ID
          Name
          Sex
          Age
          Height
          Weight
          Team
          NOC
          Games
          Year
          Season
          City
          Sport
          Event
          Medal

2-Provide an overview of the project goals and the motivation for it.

	Project goal is to clean the data from the null values and the outliers.
	
	The extracted data can be sorted/grouped (if needed) then used to answer our research questions about the dataset.
	
	The results are also visualized in various graphs and plots for easy interpretation.

3-Descriptive steps used for the work done in this milestone.
	
	Dataset loaded from csv file into variable.
	
	Dataset cleaned for outliers for weight and height attributes.
	
	Dataset null values are cleaned according to the research question being asked (in order not to delete valuable information relative to the question).
	
	Dataset sorted and conditions applied in order to generate the data we need to answer our question.
	
	A suitable visualization method is used according to the output data.
	
	
4-Data exploration questions

	Question 1: Number of male participants for the top 10 countries (based on amount of male particpants sent)
		
		Bar Graph: Comparing between the count of the male participants for different groups (Top 10 countries)
	
	Question 2: Height distribution for all Olympians throughout the years
	
		Box Plot: Used to show the distribution of Height data for olympians. (Shows quartiles and outliers)
	
	Question 3: Number of Male/Female participants over the Years
	
		Line Graph: Track changes of male and female participants each year over time.
	
	Question 4: Top 5 performing countries in terms of medals and their medal distribution
	
		Stacked Bar Chart: Showing comparisons between medal categories for the top 5 performing countries.
	
	Question 5: Which Cities hosted the games more than once
	
		Bar Graph: Displaying different countries that hosted the Olympics more than once, and how many times they were hosted.

Milestone 2

1- Overview of the used dataset
	
	athelete_events was merged with noc_regions based on the "NOC" column to obtain a new dataset that is to be used in this milestone.
	The newly added column is named "region" and contains the region name based on the "NOC"
	
2- Steps performed
	
	Data was cleaned according to the questions being asked.
	Any relevant info was cleaned against missing values so it doesn't affect the results.

3- Features
	
	The first feature that was added was the BMI and obesity indicator.
	BMI was calculated based on the Height and Weight of athletes and the obesity indicator was based on the BMI calculated.
	These two columns were added to the dataset.
	
	The second feature was age_group, the atheletes' ages were encoded in 4 categories (young, young-adult, middle-aged, old) based on their age range.
	The age_group column was then added to the dataset.

4- Newly Added Questions
	
	Question 6: Top 5 performing countries in terms of medals and their medal distribution (Based on region)(Similar to Question 4 in Milestone 1)
	
	Question 7:  BMI of olympians who won olympic medals
	
	Question 8: Number of medals won by each age group
		
		
5-Airflow
	
	In this milestone we started off by installing airflow on our computer(Mac OS). We followed the steps to integrate airflow on our localhost 8080. We then
	started by creating our python file that was inspired by the airflow documantation for DAG creation. The point of this task was to implement the Data 
	Cleaning, Data Tidying, and Feature Engineering on Airflow. Airflow's usage was to send the data to be processed elsewhere and to create a pipeline which
	is mainly used in Big Data applications. We started off by importing json, airflow, pandas, and numpy. Then intiated the DAG object which we will need to 	  instantiate a DAG. We started the default arguments, These args will get passed on to each operator, we can override them on a per-task basis during 
	operator initialization. Then we initiated the dag by setting its default arguments, description, schedule intervals and start date. Following the dag  
	initialization is the documentation.
	After, we implemented our functions as follows;
	1- extract and extract_2;
		in these function we read our csv file and set their destination paths.
	2- andling_outliers_Weight; 
		which handles outliers by replacing values below Q1-1QR*1.5 and values above Q3+IQR*1.5 with the median
	3- handling_outliers_Height;
		which handles outliers by replacing values below Q1-1QR*1.5 and values above Q3+IQR*1.5 with the median
	4- data_cleaning_1;
		which Drops rows where city column is empty, Removes duplicates based on games, Sort values, Count Sorted Values
	5- merge_data;
		which uses merge function by setting how='inner', and Saves output to csv in path
	6- data_cleaning_2;
		which handles data cleaning by Dropping the column 'notes' as it will not be used and it contains a lot of missing values, then Removing the columns that contain irrelevant information for the question we want to answer
	
	7- feature_eng_1;
		which Removes duplicate olympians based on ID, Removes Columns irrelevant to BMW calculation, Calculates BMI, Obesity Indicator based on BMI, then Removes any olympians who have not won medals
	8- feature_eng_2;
		which handles the Number of medals won by each age group, by Encoding Age groups
	
	Then we created the dag nodes by setting the python operator, unique id for each node, python's callable function.
	Last we handled the dependencies of our implemented functions.
	

