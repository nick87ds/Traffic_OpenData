import pandas as pd
from google.transit import gtfs_realtime_pb2
import urllib.request 
from datetime import datetime
import pytz

import json

# id_tragitto = {
#     "72386" : "TERMINI (MA-MB-FS)",
#     "72044" : "TERMINI (MA-MB-FS)",
#     "73365" : "TERMINI (MA-MB-FS)",
#     "70031" : "REPUBBLICA (MA)",
#     "73992" : "P.ZA STAZIONE S. PIETRO (FL)",
#     "70078" : "P.ZA VENEZIA",
#     "72983" : "P.ZA VENEZIA",
#     "70084" : "NAZIONALE/TORINO",
#     "70032" : "NAZIONALE/TORINO"

# }


with open('id_paline_fermata.json', 'r') as fp:
    id_tragitto = json.load(fp)


rome_timezone = pytz.timezone('Europe/Rome')




def get_datetime_now():

    rome_timezone = pytz.timezone('Europe/Rome')
    utc_now = datetime.utcnow().replace(tzinfo=pytz.utc)
    rome_now = utc_now.astimezone(rome_timezone)

    return rome_now

def download_extract_data():

    feed = gtfs_realtime_pb2.FeedMessage() # type: ignore
    pb_url = "https://dati.comune.roma.it/catalog/dataset/a7dadb4a-66ae-4eff-8ded-a102064702ba/resource/bf7577b5-ed26-4f50-a590-38b8ed4d2827/download/rome_trip_updates.pb"

    response = urllib.request.urlopen(pb_url)
    feed.ParseFromString(response.read())

    # print('There are {} buses in the dataset.'.format(len(feed.entity)))


    lista_fermate = []

    for bus in feed.entity:
        # print(f"Bus_id: {bus.id} -> Cancellato: {bus.is_deleted}")
        # print(f"\tLinea: {bus.trip_update.trip.route_id} -> Direzione: {bus.trip_update.trip.direction_id} -> Start_time: {bus.trip_update.trip.start_time}")
        # print(f"\tId_Veicolo: {bus.trip_update.vehicle.id}")

        for fermata in bus.trip_update.stop_time_update:

            dict_fermata = {}

            dict_fermata['Bus_ID'] = bus.id
            dict_fermata['Cancellato'] = bus.is_deleted
            dict_fermata['Linea'] = bus.trip_update.trip.route_id
            dict_fermata['Direzione'] = bus.trip_update.trip.direction_id
            dict_fermata['Start_Viaggio'] = bus.trip_update.trip.start_time
            dict_fermata['ID_Veicolo'] = bus.trip_update.vehicle.id

            dict_fermata['N_Fermata'] = fermata.stop_sequence
            dict_fermata['ID_Fermata'] = fermata.stop_id
            dict_fermata['Scheduled'] = fermata.schedule_relationship

            dict_fermata['Arrivo'] = 0 
            dict_fermata['Partenza'] = 0

            # print(f"\t\tN_fermata: {fermata.stop_sequence} -> Id_fermata: {fermata.stop_id} -> Scheduled: {fermata.schedule_relationship}")

            if fermata.arrival is not None:
                # print(f"\t\t\tArrivo: {fermata.arrival.time}")
                dict_fermata['Arrivo'] = fermata.arrival.time


            if fermata.departure is not None:
                # print(f"\t\t\tPartenza: {fermata.departure.time}")
                dict_fermata['Partenza'] = fermata.departure.time

            lista_fermate.append(dict_fermata)

    df = pd.DataFrame(lista_fermate)

    return df


def transform_data(df:pd.DataFrame, id_tragitto:dict, rome_timezone=rome_timezone):

    df['Arrivo'] = pd.to_datetime(df['Arrivo'].values, unit='s')
    df['Partenza'] = pd.to_datetime(df['Partenza'].values, unit='s')

    df['Arrivo'] = df['Arrivo'].dt.tz_localize('UTC').dt.tz_convert(rome_timezone)
    df['Partenza'] = df['Partenza'].dt.tz_localize('UTC').dt.tz_convert(rome_timezone)

    df['Fermata'] = df['ID_Fermata'].map(id_tragitto)

    return df

def get_fermata(dict_fermate:dict, nome_fermata:str) -> list:

    df_tragitto = pd.DataFrame.from_dict(dict_fermate, \
                                         orient='index', columns=['nome_fermata'])

    mask = df_tragitto.nome_fermata.str.contains(nome_fermata,  case=False)
    lista_fermate = df_tragitto[mask].nome_fermata.unique().tolist()

    return lista_fermate





def prepare_for_table(df:pd.DataFrame, rome_now, Fermate_ritorno, Destinazioni_ritorno):
    
    # Fermate_ritorno = ["P.ZA VENEZIA", "ARA COELI/PIAZZA VENEZIA"]
    # Destinazioni_ritorno = ["TERMINI (MA-MB-FS)", "NAZIONALE/TORINO"]

    df_valid = df.query("Arrivo > @rome_now and not Cancellato").dropna().copy()

    # Raggruppa e filtra solo i 'Bus_ID' con almeno 2 fermate
    df_valid_grouped = df_valid.groupby('Bus_ID')
    df_valid_grouped = df_valid_grouped.filter(lambda x: len(x) > 1).copy()

    # Raggruppa nuovamente per prendere soltanto la prima e l'ultima fermata
    df_valid_grouped = df_valid_grouped.groupby('Bus_ID')
    first_items = df_valid_grouped.first().reset_index()
    last_items = df_valid_grouped.last().reset_index()


    first_items = first_items.rename(columns={'Arrivo':'Orario_Fermata', 'Fermata':'Prossima_Fermata'})#.set_index('Bus_ID')
    last_items = last_items.rename(columns={'Arrivo':'Orario_Arrivo', 'Fermata':'Destinazione_Fermata'})#.set_index('Bus_ID')

    df_total = first_items.merge(last_items, on=['Bus_ID','Linea'])

    colonne = ['Bus_ID', 'Linea', 'Prossima_Fermata', 'Orario_Fermata', 'Destinazione_Fermata', 'Orario_Arrivo']

    df_finale = df_total[colonne].query("Destinazione_Fermata in @Destinazioni_ritorno and \
                            Prossima_Fermata in @Fermate_ritorno")
    

    df_finale.sort_values(by=['Orario_Fermata'], inplace=True)
    
    df_finale['Orario_Fermata'] = pd.to_datetime(df_finale['Orario_Fermata'])
    df_finale['Orario_Fermata'] = df_finale['Orario_Fermata'].dt.strftime('%H:%M')
    df_finale['Orario_Arrivo'] = pd.to_datetime(df_finale['Orario_Arrivo'])
    df_finale['Orario_Arrivo'] = df_finale['Orario_Arrivo'].dt.strftime('%H:%M')

    
    return df_finale