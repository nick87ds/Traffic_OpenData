import streamlit as st
import pandas as pd
import etl as etl


def text_input_form():
    str_fermate = st.text_input('Inserisci un testo per filtrare la o le fermate di interesse separate da |', '')

    if len(str_fermate) == 0:
        str_fermate = 'Venez'

    return str_fermate


def text_input_form_destination():
    str_fermate = st.text_input('Inserisci un testo per filtrare la o le fermate di destinazione separate da |', '')

    if len(str_fermate) == 0:
        str_fermate = 'TERMINI \(MA-MB-FS\)|Torino'

    return str_fermate


def main():

    st.title('Roma Bus')

    selected_paline_name = text_input_form()
    selected_paline_destination_name = text_input_form_destination()

    button_clicked = st.button('Display Bus')

    if button_clicked:

        Fermate_ritorno = etl.get_fermata(etl.id_tragitto, selected_paline_name)
        Destinazioni_ritorno = etl.get_fermata(etl.id_tragitto, selected_paline_destination_name)

        df = etl.download_extract_data()

        rome_now = etl.get_datetime_now()

        df = etl.transform_data(df, 
                                id_tragitto=etl.id_tragitto, 
                                rome_timezone=etl.rome_timezone)
        
        df = etl.prepare_for_table(df, rome_now, Fermate_ritorno, Destinazioni_ritorno)

        st.markdown("---")
        st.header('Tabella Bus')

        st.dataframe(df)


if __name__ == '__main__':
    main()
