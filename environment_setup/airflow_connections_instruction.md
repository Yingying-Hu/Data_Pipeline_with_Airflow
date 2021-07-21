# Add Airflow Connections

This is an instruction for using Airflow's UI to configure your AWS credentials and connection to Redshift.
1. To go to the Airflow UI
  
2. lick on the Admin tab and select Connections.  
![airflow](https://video.udacity-data.com/topher/2019/February/5c5aaca1_admin-connections/admin-connections.png)  
    
3. Under Connections, select Create.  
![airflow](https://video.udacity-data.com/topher/2019/February/5c5aad2d_create-connection/create-connection.png)
  
4. On the create connection page, enter the following values:  
* **Conn Id**: Enter ```aws_credentials```.
* **Conn Type**: Enter ```Amazon Web Services```.
* **Login**: Enter your **Access key ID** from the IAM User credentials you downloaded earlier.
* **Password**: Enter your **Secret access key** from the IAM User credentials you downloaded earlier.  
![aws](https://video.udacity-data.com/topher/2019/February/5c5aaefe_connection-aws-credentials/connection-aws-credentials.png)  
  
5. On the next create connection page, enter the following values:

* **Conn Id**: Enter ```redshift```.
* **Conn Type**: Enter ```Postgres```.
* **Host**: Enter the endpoint of your Redshift cluster, excluding the port at the end. You can find this by selecting your cluster in the **Clusters** page of the Amazon Redshift console. See where this is located in the screenshot below. IMPORTANT: Make sure to NOT include the port at the end of the Redshift endpoint string.
* **Schema**: Enter ```dev```. This is the Redshift database you want to connect to.
* **Login**: Enter ```awsuser```.
* **Password**: Enter the password you created when launching your Redshift cluster.
* **Port**: Enter ```5439```.
 ![redshift](https://video.udacity-data.com/topher/2019/February/5c5aaf07_connection-redshift/connection-redshift.png)

 ## WARNING: Remember to DELETE your cluster each time you are finished working to avoid large, unexpected costs.