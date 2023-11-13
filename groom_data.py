import os
import pandas as pd

raw_data_path = './raw_data_test/'  # Path to raw data stored locally
gmd_data_path = './data/aq_data_test/'  # Path to groomed data

for filename in os.listdir(raw_data_path):
    if filename.endswith('.csv'):
        raw_file_path = os.path.join(raw_data_path, filename)
        gmd_file_path = os.path.join(gmd_data_path, filename)
        transit_id = filename.split('_')[0]
        transit_df = pd.read_csv(raw_file_path, low_memory=False)

        headers = transit_df.columns.to_list()
        units = transit_df.iloc[0].to_list()

        new_headers = [f'{h}_{u}' for h, u in zip(headers, units)]

        transit_df = transit_df.iloc[1:].reset_index(drop=True)
        transit_df.columns = pd.Index(new_headers)
        transit_df['Timestamp_UTC'] = pd.to_datetime(
            transit_df['Timestamp_UTC'])
        transit_df.iloc[:, 1:-2] = transit_df.iloc[:, 1:-2].astype(float)

        transit_df['Time_Tuple'] = transit_df['Timestamp_UTC'].apply(
            lambda x: x.timetuple())
        transit_df['Date'] = transit_df['Timestamp_UTC'].apply(
            lambda x: x.date())
        transit_df['Year'] = transit_df['Timestamp_UTC'].apply(
            lambda x: x.timetuple().tm_year)
        transit_df['Month'] = transit_df['Timestamp_UTC'].apply(
            lambda x: x.timetuple().tm_mon)
        transit_df['Day_of_Year'] = transit_df['Timestamp_UTC'].apply(
            lambda x: x.timetuple().tm_yday)
        transit_df['Day_of_Month'] = transit_df['Timestamp_UTC'].apply(
            lambda x: x.timetuple().tm_mday)
        transit_df['Minute'] = transit_df['Timestamp_UTC'].apply(
            lambda x: x.timetuple().tm_min)
        transit_df['Hour'] = transit_df['Timestamp_UTC'].apply(
            lambda x: x.timetuple().tm_hour)
        transit_df['Transit_ID'] = transit_id
        pm25_cols = transit_df.filter(like='PM2.5_Concentration_ug/m3')
        ozone_cols = transit_df.filter(like='Ozone_Concentration_ppbv')
        transit_df_filt = transit_df[
            (pm25_cols != -9999.00).all(axis=1) &
            (ozone_cols != -9999.00).all(axis=1) &
            (transit_df['Latitude_ddeg'] != -9999.00) &
            (transit_df['Longitude_ddeg'] != -9999.00)]

        transit_df_gmd = transit_df_filt.iloc[::30, :].reset_index(drop=True)
        transit_df_gmd.to_csv(gmd_file_path, index=False)
        print(f'Groomed: {filename}')
