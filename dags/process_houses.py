import os
import pandas as pd
import shutil
from datetime import datetime

# Définir les chemins
input_folder = 'toProcess'
processed_folder = 'already_processed'
output_folder = 'result'

# Fonction pour traiter les fichiers CSV
def process_csv_files():
    for file_name in os.listdir(input_folder):
        if file_name.endswith('.csv'):
            # Charger le fichier CSV
            file_path = os.path.join(input_folder, file_name)
            df = pd.read_csv(file_path)

            # Calculer la moyenne des prix
            average_price = df['price'].mean()

            # Sauvegarder le résultat dans un fichier CSV
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f'result_{timestamp}.csv'
            output_path = os.path.join(output_folder, output_file)

            result_df = pd.DataFrame({'average_price': [average_price]})
            result_df.to_csv(output_path, index=False)

            # Déplacer le fichier traité
            shutil.move(file_path, os.path.join(processed_folder, file_name))

if __name__ == '__main__':
    process_csv_files()
