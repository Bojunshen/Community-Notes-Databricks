import pandas as pd
from bs4 import BeautifulSoup as bs
import requests
from datetime import date, timedelta
from pathlib import Path
import io, zipfile
import json
from concurrent.futures import ThreadPoolExecutor
import os
import shutil
import duckdb


# All the data names on the website
dataname_dict = {
    'notes': 'notes',
    'noteRatings': 'ratings',
    'noteStatusHistory': 'noteStatusHistory',
    'userEnrollment': "userEnrollment",
    'batSignals': 'batSignals'
            }

#Fetch all data wuth clicking that link, the link will renew daily
def get_notes_data(day: date, dataset_name: str, dataset_subname: str, out_dir="community_notes", max_iter = 100):
    extracted = []
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    for i in range(max_iter): # the file is named with batch number so simply hit it with itered nums until error then stop
        url = (f"https://ton.twimg.com/birdwatch-public-data/{day:%Y/%m/%d}/{dataset_name}/{dataset_subname}-{i:05d}.zip") # Just click the download link today directly
        r = requests.get(url, timeout=600) # set the timeout to ten mins for possible huge files
        if r.status_code != 200:
            print(f'num{i}, {r.status_code}')
            break
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            z.extractall(out_dir)
            extracted.extend(z.namelist())
    return extracted



def find_and_download(k, v):
    d = date.today()   # ← find data who loaded today only
    hit = get_notes_data(d, k, v)
    if hit:
        print("Downloaded:", hit)
    else:
        print(f'Today {d}, No data for {v}')
    return hit


def return_old_path(name: str, day = date.today(), data_dir="Concat"):
    for back in range(1, 8):# try to find data in the stored location, track the latest version til the last 7 days maximum
        lastday = day - timedelta(days=back) # Note: there could be days when no new data arrived to the website
        old_path = f'{data_dir}/{name}_{lastday:%Y-%m-%d}.parquet'
        if Path(old_path).exists(): 
            return old_path, lastday
    return None, None


def concat_and_process(name:str, day: date = date.today(), notes_path=Path('community_notes'), out_dir="Concat", max_iter = 100):  # I need speed so I use pyarrow
    data_list = list() # Find all the files for a category of dataframe  and concat them into a file
    for i in range(max_iter):
        print(f'start finding data {i} ...')
        try:
            df = pd.read_csv(f'{notes_path}/{name}-{i:05d}.tsv', sep='\t', engine='pyarrow')
            data_list.append(df)
            print(f'Found {name}-{i:05d}.tsv')
        except FileNotFoundError as e:
            print(e)
            break

    print('-'*20)
    print('File has been found, start concating...')
    try:
        concat_dataset = pd.concat(data_list, axis=0, ignore_index=True)
    except Exception as e:
        print('Concat process error:', e)
        return None
    # Dropping duplicates after concatenation
    print('-'*20)
    print('finish concating! Droping duplicates ...')
    try:
        dropdu_data = concat_dataset.drop_duplicates(keep="first")
        print(f"New data:{len(dropdu_data):,}")
    except Exception as e:
        print('Drop duplicates error: ', e)
        return None

    # drop past records, only new rows are added to the old file, and output as today's data
    print('-'*20)
    print(f'Start dropping past duplicates ...')
    def drop_inpast(new_df = dropdu_data, name=name, data_dir=out_dir): # I only leave new dataset and store them as separated file with date
        old_path, lastday = return_old_path(name=name, day=day, data_dir=data_dir)
        if old_path is None:
            print(f'No {name} file in past 7 days, using {day} data as baseline')
            return new_df
        
        if name == 'ratings':  # ratings to chunky so only record the time column and remain the latest rows (Currently not used since I use duck db)
            cutoff = pd.read_parquet(old_path, columns=['createdAtMillis'], engine='pyarrow')['createdAtMillis'].max()
            new_df = new_df[new_df['createdAtMillis'] > cutoff]
            print(f"Cutoff: {pd.to_datetime(cutoff, unit='ms')}, remained new data:{len(new_df):,}")
            return new_df

        old_df = pd.read_parquet(old_path, engine='pyarrow')  # ←
        print(f"Old data:{len(old_df):,}, yesterday= {lastday}")

        old_hash = pd.util.hash_pandas_object(old_df, index=False)
        new_hash = pd.util.hash_pandas_object(new_df, index=False)
        new_df = new_df[(~new_hash.isin(old_hash) & ~new_hash.duplicated()).values]
        print(f"Remained new data:{len(new_df):,}")
        return pd.concat([old_df, new_df], ignore_index=True)

    try:
        new_data = drop_inpast()
    except Exception as e:
        print('Drop past duplicates FAILED: ',e)
        new_data = dropdu_data

    print('-'*20)
    print(f'Start writing file to {name}_{day:%Y-%m-%d}.parquet ...')
    Path(out_dir).mkdir(parents=True, exist_ok=True)  
    out_path = Path(out_dir) / f'{name}_{day:%Y-%m-%d}.parquet' # lower disk space, write new data to the concat folder in parquet format
    new_data.to_parquet(out_path, index=False)

    return out_path  


def concat_and_process_duckdb(name:str, day: date = date.today(), notes_path=Path('community_notes'), out_dir="Concat"):
    total_gb = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') / 1e9
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{int(total_gb * 0.8)}GB'")   # Use at most 80% of overall memory
    con.execute("SET temp_directory='duckdb_tmp'")              # if exceed, I store in duckdb
    con.execute("SET preserve_insertion_order=false")           # Leave the processed steps to reduce memory 
    print(f'{name}: memory limit {int(total_gb * 0.8)} GB of {total_gb:.0f} GB')

    files = [str(f) for f in sorted(Path(notes_path).glob(f'{name}-*.tsv'))]
    if not files:
        print(f'No {name} TSV found')
        return None
    src = f"read_csv({files}, delim='\\t', header=true, union_by_name=true)"

    old_path, lastday = return_old_path(name=name, day=day, data_dir=out_dir)
    if old_path is not None:                                    # Only leavwe the new version of data
        print(f'Cutoff from {old_path} ({lastday})')
        query = f"""SELECT DISTINCT * FROM {src}
                    WHERE createdAtMillis > (SELECT max(createdAtMillis) FROM read_parquet('{old_path}'))"""
    else:                                                       # Store the complete file if no previous version exist in 7 days
        print(f'No {name} file in past 7 days, using {day} data as baseline')
        query = f"SELECT DISTINCT * FROM {src}"

    Path(out_dir).mkdir(parents=True, exist_ok=True)
    out_path = Path(out_dir) / f'{name}_{day:%Y-%m-%d}.parquet'
    con.execute(f"COPY ({query}) TO '{out_path}' (FORMAT parquet)")
    n = con.execute(f"SELECT count(*) FROM read_parquet('{out_path}')").fetchone()[0]
    print(f'Wrote {out_path} ({n:,} rows)')
    con.close()
    return out_path



def main():
    os.chdir(Path(__file__).parent) # lock the working directory
    with ThreadPoolExecutor(max_workers=5) as ex: #execute hitting (download), by datanames
        results = dict(zip(dataname_dict, ex.map(lambda kv: find_and_download(*kv), dataname_dict.items())))
    for k, v in dataname_dict.items():
        if not results[k]:                        # Continue if no downloads today
            print(f'Skip {v}: no data downloaded today')
            continue
        # out_path = concat_and_process(v)
        out_path = concat_and_process_duckdb(v) if v == 'ratings' else concat_and_process(v)
        print(out_path)
    shutil.rmtree('community_notes', ignore_errors=True)   # Delete the whole folder for catching current date data (Reduce disk space and memory)
    print('community_notes folder removed')

# def main():
#     import os
#     print('cwd:', os.getcwd())
#     print('Concat:', Path('Concat').resolve())

if __name__ == "__main__":
    main()



