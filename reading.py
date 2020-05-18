import pandas as pd


class reading:
    def __init__(self):
        #df = pd.read_csv("C:/Users/User/Desktop/NUI/sem2/Research Topics/yellow_tripdata_2018-01.csv")

        #print(df['tpep_pickup_datetime'].dtype)
        #print(df.head())

        #df['tpep_pickup_datetime'] = pd.to_datetime(df.tpep_pickup_datetime)
        # print(df['tpep_pickup_datetime'].dtype)
        # df = df.sort_values(by=['tpep_pickup_datetime'])
        # print(df.head())
        # export_csv = df.to_csv("C:/Users/User/Desktop/NUI/sem2/Research Topics/Yellow_TripData.csv", index=None,
        #                      header=True)

        zone_lookup = pd.read_csv("C:/Users/User/Desktop/NUI/sem2/Research Topics/taxi+_zone_lookup.csv")
        zone_lookup['Borough_Zone'] = zone_lookup['Borough'] + ', ' + zone_lookup['Zone']
        zone_lookup = zone_lookup.drop(['Borough', 'Zone', 'service_zone'], axis=1)


if __name__ == '__main__':
    my_data = reading()
    print(my_data) 
