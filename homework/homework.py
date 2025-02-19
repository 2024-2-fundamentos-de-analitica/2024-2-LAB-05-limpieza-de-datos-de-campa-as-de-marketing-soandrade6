"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import pandas as pd
import zipfile
import os
import glob

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months

    """
    input_folder = "files/input/"
    output_folder = "files/output/"
    
    os.makedirs(output_folder, exist_ok=True)

    zip_files = glob.glob(os.path.join(input_folder, "*.csv.zip"))
    dfs = []

    for zip_file in zip_files:
        with zipfile.ZipFile(zip_file, 'r') as z:
            csv_filename = z.namelist()[0]  
            with z.open(csv_filename) as f:
                df = pd.read_csv(f, sep=",", index_col=0) 
                dfs.append(df)

    if not dfs:
        print("Error: No se encontraron archivos CSV en los ZIP.")
        return

    data = pd.concat(dfs, ignore_index=True)

    data.columns = data.columns.str.strip()  
    print("Columnas disponibles en el DataFrame:", data.columns.tolist())

    expected_columns = [
        "client_id", "age", "job", "marital", "education", "credit_default",
        "mortgage", "month", "day", "contact_duration", "number_contacts",
        "previous_campaign_contacts", "previous_outcome", "cons_price_idx",
        "euribor_three_months", "campaign_outcome"
    ]
    
    missing_columns = [col for col in expected_columns if col not in data.columns]
    if missing_columns:
        print(f"Error: Columnas faltantes en el DataFrame: {missing_columns}")
        return

    client_df = data[["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]].copy()
    client_df["job"] = client_df["job"].str.replace(".", "", regex=False).str.replace("-", "_", regex=False)
    client_df["education"] = client_df["education"].str.replace(".", "_", regex=False).replace("unknown", pd.NA)
    client_df["credit_default"] = client_df["credit_default"].apply(lambda x: 1 if x == "yes" else 0)
    client_df["mortgage"] = client_df["mortgage"].apply(lambda x: 1 if x == "yes" else 0)
    client_df.to_csv(os.path.join(output_folder, "client.csv"), index=False)

    campaign_df = data[[
        "client_id", "number_contacts", "contact_duration",
        "previous_campaign_contacts", "previous_outcome", "campaign_outcome",
        "day", "month"
    ]].copy()

    campaign_df["previous_outcome"] = campaign_df["previous_outcome"].apply(lambda x: 1 if x == "success" else 0)
    campaign_df["campaign_outcome"] = campaign_df["campaign_outcome"].apply(lambda x: 1 if x == "yes" else 0)

    month_mapping = {
        "jan": "01", "feb": "02", "mar": "03", "apr": "04",
        "may": "05", "jun": "06", "jul": "07", "aug": "08",
        "sep": "09", "oct": "10", "nov": "11", "dec": "12"
    }
    campaign_df["month"] = campaign_df["month"].map(month_mapping)
    campaign_df["last_contact_date"] = "2022-" + campaign_df["month"] + "-" + campaign_df["day"].astype(str)
    campaign_df.drop(columns=["day", "month"], inplace=True)
    campaign_df.to_csv(os.path.join(output_folder, "campaign.csv"), index=False)

    economics_df = data[["client_id", "cons_price_idx", "euribor_three_months"]].copy()
    economics_df.to_csv(os.path.join(output_folder, "economics.csv"), index=False)

    print("Archivos generados exitosamente en", output_folder)

if __name__ == "__main__":
    clean_campaign_data()