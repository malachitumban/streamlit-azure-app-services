import streamlit as st
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.filedatalake import DataLakeServiceClient
import pandas as pd

# Function to get secret from Azure Key Vault
def get_secret_from_key_vault(vault_url, secret_name, tenant_id, client_id, client_secret):
    credential = ClientSecretCredential(tenant_id, client_id, client_secret)
    secret_client = SecretClient(vault_url=vault_url, credential=credential)
    secret = secret_client.get_secret(secret_name)
    return secret.value

# Function to initialize the DataLakeServiceClient
def initialize_storage_account(storage_account_name, account_key):
    try:
        datalake_service_client = DataLakeServiceClient(
            account_url=f"https://{storage_account_name}.dfs.core.windows.net",
            credential=account_key
        )
        return datalake_service_client
    except Exception as e:
        st.error(f"Error initializing storage account: {e}")
        return None

# Function to list files and their metadata
def list_files_and_metadata(datalake_service_client, file_system_name, directory_path):
    try:
        file_system_client = datalake_service_client.get_file_system_client(file_system_name)
        paths = file_system_client.get_paths(path=directory_path)
        
        file_data = []
        for path in paths:
            if not path.is_directory:  # Only process files
                file_client = file_system_client.get_file_client(path.name)
                properties = file_client.get_file_properties()
                file_data.append({
                    "File Name": path.name,
                    "Size (Bytes)": properties.get('content_length', 0),  # Default to 0 if not present
                    "Last Modified": properties.get('last_modified', None)  # Default to None if not present
                })
            # else:
            #     # Optionally, log or display directories if needed
            #     st.write(f"Skipping directory: {path.name}")
        
        return pd.DataFrame(file_data)
    except Exception as e:
        st.error(f"Error listing files and metadata: {e}")
        return pd.DataFrame()

# Streamlit app
st.title("ALPINE - ADLS File Metadata Viewer")

vault_url = st.text_input("Key Vault URL")
secret_name = st.text_input("Secret Name for Storage Account Key")
tenant_id = st.text_input("Tenant ID")
client_id = st.text_input("Client ID")
client_secret = st.text_input("Client Secret", type="password")
storage_account_name = st.text_input("Storage Account Name")
file_system_name = st.text_input("File System Name")
directory_path = st.text_input("Directory Path")

if st.button("List Files"):
    if vault_url and secret_name and tenant_id and client_id and client_secret and storage_account_name and file_system_name and directory_path:
        account_key = get_secret_from_key_vault(vault_url, secret_name, tenant_id, client_id, client_secret)
        datalake_service_client = initialize_storage_account(storage_account_name, account_key)
        if datalake_service_client:
            file_metadata_df = list_files_and_metadata(datalake_service_client, file_system_name, directory_path)
            if not file_metadata_df.empty:
                st.dataframe(file_metadata_df)
            else:
                st.write("No files found or error retrieving files.")
    else:
        st.error("Please provide all required inputs.")
