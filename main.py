import pandas as pd
import numpy as np
from datetime import timedelta


def main():
    df_results = pd.DataFrame()
    df = pd.read_csv('calls_without_target.csv', sep=';')
    df['call_ts'] = pd.to_datetime(df['call_ts'], format="%Y/%m/%d %H:%M:%S")
    df = df.sort_values(by='call_ts')
    customer_id_list = df['customer_id'].unique()
    for customer_id in customer_id_list:
        df_customer = df[df['customer_id'] == customer_id].reset_index(drop=True)
        there_are_more_than_one_call = len(df_customer.index) > 1
        if there_are_more_than_one_call:
            df_processed_rows = process_customer_calls(df_customer)
        else:
            df_processed_rows = add_none_columns(df_customer)

        df_results = pd.concat([df_results, df_processed_rows])

    df_results = df_results.sort_values(by='call_ts')
    df_results.to_csv('out.csv')


def process_customer_calls(df_customer):
    df_processed_rows = pd.DataFrame()
    while not df_customer.empty:
        row = df_customer.loc[0]
        two_days_mask = (
                df_customer['call_ts'] < (row['call_ts'] + timedelta(days=2))
        )
        df_aux = df_customer[two_days_mask]
        there_are_more_than_one_call_in_two_days = len(df_aux.index) > 1
        if there_are_more_than_one_call_in_two_days:
            df_aux = add_columns(df_aux)
        else:
            df_aux = add_none_columns(df_aux)

        df_processed_rows = pd.concat([df_aux, df_processed_rows])
        rows_to_drop = len(df_aux.index)
        df_customer = df_customer.drop(
            index=df_customer.index[:rows_to_drop]
        )
        df_customer = df_customer.reset_index(drop=True)

    return df_processed_rows


def add_columns(df_customer):
    def subtract_timestamps(time_stamp):
        result = time_stamp - first_call_hour
        result = "" if time_stamp - first_call_hour == 0 else result
        return result

    precursor_call_id = df_customer.loc[0]['call_id']
    first_call_hour = df_customer.loc[0]['call_ts']
    df_customer = df_customer.assign(is_precursor=np.where(
        df_customer.index == 0,  'Y', 'N'
    ))
    df_customer = df_customer.assign(is_recall=np.where(
        df_customer.index != 0, 'Y', 'N'
    ))
    df_customer = df_customer.assign(precursor_call_id=np.where(
        df_customer.index == 0, " ", precursor_call_id
    ))
    df_customer['hours_rom_first_call'] = df_customer.call_ts.apply(
        subtract_timestamps
    )
    return df_customer


def add_none_columns(df_customer):
    df_customer = df_customer.assign(is_precursor='N')
    df_customer = df_customer.assign(is_recall='N')
    df_customer = df_customer.assign(precursor_call_id=None)
    df_customer = df_customer.assign(hours_rom_first_call=None)
    return df_customer


if __name__ == '__main__':
    main()
