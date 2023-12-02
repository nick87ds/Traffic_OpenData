import streamlit as st
import pandas as pd
import etl as etl

# def display_table1(filtered_values):
#     data1 = {
#         'Column 1': [1, 2, 3, 4],
#         'Column 2': ['A', 'B', 'C', 'D']
#     }
#     df1 = pd.DataFrame(data1)
    
#     if filtered_values:
#         filtered_df = df1[df1['Column 2'].isin(filtered_values)]
#         st.dataframe(filtered_df)
#     else:
#         st.dataframe(df1)

st.title('Streamlit Template')

# options = ['A', 'B', 'C']
# default_value = ['A']  # Default value set to 'A'
# selected_values = st.multiselect('Select values to filter Table 1', options, default=default_value)

button_clicked = st.button('Display Tables')

if button_clicked:

    df = etl.download_extract_data()

    rome_now = etl.get_datetime_now()

    df = etl.transform_data(df, 
                            id_tragitto=etl.id_tragitto, 
                            rome_timezone=etl.rome_timezone)
    
    df = etl.prepare_for_table(df, rome_now)

    st.markdown("---")
    st.header('Table 1')
    # display_table1(selected_values)

    st.dataframe(df)
