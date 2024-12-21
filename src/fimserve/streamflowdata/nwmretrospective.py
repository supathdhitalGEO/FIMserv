import os
from pathlib import Path
import pandas as pd
from datetime import datetime
import teehr.fetching.nwm.retrospective_points as nwm_retro

from ..datadownload import setup_directories


def getdischargeforspecifiedtime(
    retrospective_dir, location_ids, specific_date, data_dir, huc, date_type
):
    retrospective_dir = Path(retrospective_dir)
    all_data = pd.DataFrame()

    # Loop through all parquet files in the directory
    for file in retrospective_dir.glob("*.parquet"):
        df = pd.read_parquet(file)
        all_data = pd.concat([all_data, df], ignore_index=True)

    df["value_time"] = pd.to_datetime(df["value_time"])

    locationID_df = pd.read_csv(location_ids)
    location_ids = [f"nwm30-{int(fid)}" for fid in locationID_df["feature_id"]]

    specific_date = pd.to_datetime(specific_date)
    if date_type == "date":
        filtered_df = df[
            (df["location_id"].isin(location_ids))
            & (df["value_time"].dt.date == specific_date.date())
        ]
        filtered_df["feature_id"] = filtered_df["location_id"].str.replace("nwm30-", "")
        discharge_data = (
            filtered_df.groupby("feature_id")["value"]
            .mean()
            .reset_index()
            .rename(columns={"value": "discharge"})
        )
        formatted_datetime = specific_date.strftime("%Y%m%d")
    else:
        filtered_df = df[
            (df["location_id"].isin(location_ids)) & (df["value_time"] == specific_date)
        ]
        filtered_df.loc[:, "feature_id"] = filtered_df["location_id"].str.replace(
            "nwm30-", ""
        )
        discharge_data = filtered_df[["feature_id", "value"]].rename(
            columns={"value": "discharge"}
        )
        formatted_datetime = specific_date.strftime("%Y%m%d%H%M%S")

    # Save to a CSV file with the date and HUC as filename
    finalHANDdischarge_dir = os.path.join(data_dir, f"{formatted_datetime}_{huc}.csv")
    discharge_data.to_csv(finalHANDdischarge_dir, index=False)
    print(f"Discharge values saved to {finalHANDdischarge_dir}")


def getnwm_discharge(
    start_date,
    end_date,
    fids,
    output_root,
    nwm_version="nwm30",
    variable_name="streamflow",
):
    output_dir = Path(output_root) / "discharge" / f"{nwm_version}_retrospective"
    output_dir.mkdir(parents=True, exist_ok=True)

    location_ids_df = pd.read_csv(fids)
    location_ids = location_ids_df["feature_id"].tolist()

    nwm_retro.nwm_retro_to_parquet(
        nwm_version=nwm_version,
        variable_name=variable_name,
        start_date=start_date,
        end_date=end_date,
        location_ids=location_ids,
        output_parquet_dir=output_dir,
    )
    print(f"NWM discharge data saved to {output_dir}.")


def determinedatatimeformat(date_str):
    if isinstance(date_str, pd.Timestamp):
        return "datetime"
    try:
        parsed_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        return "date"
    except ValueError:
        try:
            parsed_datetime = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
            return "datetime"
        except ValueError:
            return "invalid"


def getNWMretrospectivedata(start_date, end_date, huc, value_times=None):
    code_dir, data_dir, output_dir = setup_directories()

    HUC_dir = os.path.join(output_dir, f"flood_{huc}")
    featureID_dir = os.path.join(HUC_dir, f"feature_IDs.csv")
    getnwm_discharge(start_date, end_date, featureID_dir, HUC_dir)
    if value_times:
        retrospective_dir = os.path.join(HUC_dir, "discharge", "nwm30_retrospective")
        for time in value_times:
            datetype = determinedatatimeformat(time)
            getdischargeforspecifiedtime(
                retrospective_dir, featureID_dir, time, data_dir, huc, datetype
            )
