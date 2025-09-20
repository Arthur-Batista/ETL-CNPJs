import os
import zipfile

def compactar_csv_para_zip(caminho_csv, caminho_zip=None):
    """
    Compacta um arquivo CSV em um arquivo ZIP com alto nível de compressão.
    
    :param caminho_csv: Caminho completo do arquivo CSV.
    :param caminho_zip: Caminho completo do arquivo ZIP (opcional).
    """
    if not os.path.isfile(caminho_csv):
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho_csv}")
    
    if caminho_zip is None:
        caminho_zip = os.path.splitext(caminho_csv)[0] + ".zip"
    
    with zipfile.ZipFile(caminho_zip, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zipf:
        zipf.write(caminho_csv, arcname=os.path.basename(caminho_csv))
    
    print(f"Arquivo compactado com sucesso: {caminho_zip}")