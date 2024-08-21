from pathlib import Path
import dask.dataframe as dd
import pandas as pd
from numpy import linspace, mean, array, diff, histogram


def get_paths() -> dict:
    """
    Retrieves paths necessary for general analysis to function.

    We assume that this python file is located inside main_dir/analysis. 
    We also assume existence of main_dir/data, and main_dir/data_intermediate.

    Returns:
    path_dict = {"main": path_to_main_directory,
                "data":{
                    "data_file_stem_n": path_to_data_file_n,
                    },
                "sample": path_to_sample_data (to use for meta),
                "store": {
                    "data_file_stem_n": path_to_store_analysis_results,
                    } (if it doesn't exist, create it) 
                }
    """
    path_dict = {}
    path_dict["main"] = Path.cwd().parent
    path_dict["data"] = {}
    for i in (path_dict["main"] / "data").iterdir():
        if i.stem == "sample":
            path_dict["sample"] = i
        else:
            path_dict["data"][i.stem] = i
    
    path_dict["store"] = {}
    for data_name in path_dict["data"].keys():
        check = path_dict["main"] / f"data_intermediate/{data_name}"
        if not check.exists():
            check.mkdir()
        path_dict["store"][data_name] = check
    return path_dict

def analyze_event_type(ddf, store_path):
    result = ddf["event_type"].value_counts().compute()
    result = pd.DataFrame(result)
    result.index.rename("Type of Event", inplace=True)
    result = result.rename(columns={
        "count":"Count",})
    result.to_csv(store_path)

def analyze_num_events_per_day(ddf, store_path):
    ddf = ddf.drop("event_time", axis=1)
    result = ddf.groupby("event_date", sort=True).count().compute()
    result = pd.DataFrame(result)
    result.index.rename("Date", inplace=True)
    result = result.rename(columns={
        "event_date": "Date",
        "event_type": "Number of Events"})
    result.to_csv(store_path)

def analyze_num_events_groupby(ddf, store_path):
    result = ddf.groupby(["event_date", "event_type"], sort=True).count().compute()
    result = pd.DataFrame(result)
    result.index.rename({"event_date": "Date",
                         "event_type": "Type of Events",},inplace=True)
    result = result.rename(columns={
        "event_date": "Date",
        "event_type": "Type of Events",
        "event_time": "Number of Events"})
    result.to_csv(store_path)

def analyze_num_distinct(ddf, store_path):
    result_dict = {}
    r0 = ddf["user_id"].nunique().compute()
    result_dict["Number of Distinct Users"] = r0
    r1 = ddf["product_id"].nunique().compute()
    result_dict["Number of Distinct Products"] = r1
    r2 = ddf["category_id"].nunique().compute()
    result_dict["Number of Distinct Categories"] = r2
    result = pd.DataFrame(result_dict, index = ["Number of Distinct Values"])
    result.to_csv(store_path)

def analyze_price(ddf, store_path):
    n_bins = 100
    price_max = ddf["price"].max().compute()
    price_min = ddf["price"].min().compute()
    bin_edges = linspace(price_min, price_max, n_bins + 1)
    hist_list = ddf["price"].map_partitions(lambda x: histogram(x, bins=bin_edges, density=True)).compute()
    hist = mean(array([h[0] for h in hist_list]), axis=0)* diff(bin_edges)
    result = pd.DataFrame({
        "Price": bin_edges[1:],
        "Density": hist,
    })
    result.to_csv(store_path, index=False)

def run_general_analysis(need:dict, analysis_dict:dict, store_path, ddf):
    for task, analyze in analysis_dict.items():
        if not need[task]:
            print(f"Skipped {task}")
            continue
        print(f"Start {task}")
        if task in ["num_events_per_day", "num_events_per_day_groupby_type"]:
            ddf_0 = ddf[["event_time", "event_type"]].copy()
            ddf_0["event_date"] = ddf_0["event_time"].dt.date
            indiv_store_path = store_path / f"{task}.csv"
            analyze(ddf_0, indiv_store_path)
        else:
            indiv_store_path = store_path / f"{task}.csv"
            analyze(ddf, indiv_store_path)
        print(f"Completed {task}")


def check_analysis_need(store_path: Path, check_list: list) -> dict:
    need = {}
    for task in check_list:
        if (store_path / f"{task}.csv").exists():
            need[task] = False
        else:
            need[task] = True
    print("Completed checking for necessary analysis")
    return need

def sample_analysis(sample_path)-> pd.DataFrame:
    df = pd.read_csv(sample_path, index_col = 0)
    df["event_time"] = pd.to_datetime(df["event_time"])
    return df

def main():
    # get all the necessary paths
    paths_dict = get_paths()
    # run a very simple procedure with the sample data to act as meta
    sample = sample_analysis(paths_dict["sample"])
    # a dictionary that maps task name to the functions responsible for them 
    analysis_dict = {
        "event_type_distribution": analyze_event_type,
        "num_of_distinct_user_product_category": analyze_num_distinct,
        "price_distribution": analyze_price,
        "num_events_per_day": analyze_num_events_per_day,
        "num_events_per_day_groupby_type": analyze_num_events_groupby,
    }
    for data_name, data_path in paths_dict["data"].items():
        need_analysis = check_analysis_need(paths_dict["store"][data_name], analysis_dict.keys())

        if not any(need_analysis.values()):
            print(f"Skipped {data_name}")
            continue
        print(f"Start analysis on {data_name}")
        ddf = dd.read_csv(data_path)
        if any([need_analysis["num_events_per_day"], need_analysis["num_events_per_day_groupby_type"]]):
            ddf["event_time"] = ddf["event_time"].map_partitions(pd.to_datetime, meta = sample["event_time"])

        run_general_analysis(need_analysis, analysis_dict, paths_dict["store"][data_name], ddf)




    

if __name__ == "__main__":
    main()