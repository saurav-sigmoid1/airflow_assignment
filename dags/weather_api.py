import os
import requests
import pandas as pd
from config import Host, API_KEY

def weather_api_method():

    url = "https://community-open-weather-map.p.rapidapi.com/weather"
    #list of states
    state_list = ['goa','uttar Pradesh','gujarat','kerala','Delhi','Panjab','Maharashtra','Rajasthan','Bihar','Tripura']
    df = pd.DataFrame(columns=["State", "Description", "Temperature", "Feels_Like_Temperature", "Min_Temperature", "Max_Temperature", "Humidity", "Clouds"])
    #iterating over statelist and append the response value in dataframe
    for state in state_list:
        querystring = {"q":state}

        headers = {
            'x-rapidapi-host': Host,
            'x-rapidapi-key': API_KEY
            }

        response = requests.get( url, headers=headers, params=querystring)
        info = response.json()
        #store the data in dataframe
        try:
            df = df.append({'State':info['name'],"Description":info['weather'][0]['description'],'Temperature':info['main']['temp'],"Feels_Like_Temperature":info['main']['feels_like'],"Min_Temperature":info['main']['temp_min'],"Max_Temperature":info['main']['temp_max'],"Humidity":info['main']['humidity'],"Clouds":info['clouds']['all']},ignore_index=True)
        except :
            print("Request limit exceeds")

    path = "/usr/local/airflow/store_files_airflow"
   #check file exist or not if exist remove and create new file else create file
    if not os.path.isfile(os.path.join(path,'/weather_data.csv')):
        df.to_csv(path+'/weather_data.csv',index=False)
    else:
        os.remove(os.path.join(path,'/weather_data.csv'))
        df.to_csv(os.path.join(path,'/weather_data.csv'),index=False)

