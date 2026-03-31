"""
Date Updated: 31 Mar, 2026
Author: Supath Dhital (sdhital@ua.edu)
"""

from __future__ import annotations
from typing import Optional, Dict, Any, List, Tuple, DefaultDict
from pathlib import Path
import os
import shutil
from collections import defaultdict

# Internal utilities
from .utils import (
    load_catalog_core,
    download_fim_assets,
    _to_date,
    _to_hour_or_none,
    _record_day,
    _record_hour_or_none,
    format_records_for_print,
    find_fims,
    _record_huc8_list,
    _folder_from_record,
    _list_prefix,
    _download,
    BUCKET,
)

from ..datadownload import DownloadHUC8, setup_directories
from ..streamflowdata.nwmretrospective import getNWMretrospectivedata
from ..runFIM import runOWPHANDFIM


class FIMService:
    """
    - query(huc8, date_input=None, file_name=None) -> strict matches + pretty listing
    - process(..., ensure_owp, generate_owp_if_missing, base_dir=None, file_name=None)
      Creates folders {CWD}/FIM_evaluation/FIM_inputs/HUC{huc}_flood{YYYYMMDD[HHMMSS]}
      Downloads ONLY the matched record(s) (and their gpkg) into that folder.
    """

    # Run setup_directories() only when actually needed.
    def _ensure_roots(self):
        if hasattr(self, "_roots_initialized"):
            return

        _, _, out_root = setup_directories()
        self.default_root = out_root
        self.owp_root = Path(os.getenv("OWP_OUT_ROOT", out_root))

        self._roots_initialized = True

    def availability(self, HUCID: str) -> str:
        from .utils import availability as _avail

        return _avail(HUCID)

    @staticmethod
    def _site_of(rec: Dict[str, Any]) -> str:
        s = rec.get("site_id") or rec.get("site")
        s = str(s or "").strip()
        return s if s else "site_unknown"

    def _find_any_owp_for_day(self, huc8: str, ymd: str) -> Optional[Path]:
        dirp = self.owp_root / f"flood_{huc8}" / f"{huc8}_inundation"
        if not dirp.exists():
            return None
        for p in sorted(dirp.glob(f"NWM_{ymd}*_{huc8}_inundation.tif")):
            return p
        return None

    # Return-period helpers
    def _flows_inputs_dir(self) -> Path:
        """
        Fixed location to store return-period flow CSVs.
        User requested: ./data/inputs
        """
        p = Path(os.getcwd()) / "data" / "inputs"
        p.mkdir(parents=True, exist_ok=True)
        return p

    def _download_return_period_flows_csv(self, huc8: str, return_period: int) -> Path:
        """
        Download the Tier_4 BLE flow CSV for this HUC8 + return period into ./data/inputs.
        This is used instead of event-based NWM retrospective download.
        """
        catalog = load_catalog_core()
        records = catalog.get("records", [])

        # Find matching records by HUC and return period (Tier_4 BLE records have no date)
        matches = find_fims(
            records=records,
            huc8=str(huc8).strip(),
            date_input=None,
            file_name=None,
            start_date=None,
            end_date=None,
            return_period=int(return_period),
            relaxed_for_print=False,
        )

        if not matches:
            raise FileNotFoundError(
                f"No benchmark record found for HUC {huc8} with return period {return_period}."
            )

        # Prefer Tier_4 records if present
        tier4 = []
        for r in matches:
            t = (
                str(r.get("tier") or r.get("quality") or "")
                .strip()
                .lower()
                .replace(" ", "")
            )
            if t.startswith("tier_4") or t.startswith("tier4"):
                tier4.append(r)
        rec = tier4[0] if tier4 else matches[0]

        # List keys in that S3 prefix/folder
        prefix = _folder_from_record(rec)
        keys = _list_prefix(prefix)

        tag = f"FLOWS_{int(return_period)}YR".upper()
        flow_keys = []
        for k in keys:
            if k.lower().endswith(".csv") and tag in os.path.basename(k).upper():
                flow_keys.append(k)

        if not flow_keys:
            raise FileNotFoundError(
                f"No {tag} CSV found under S3 prefix '{prefix}' for HUC {huc8}."
            )

        flows_dir = self._flows_inputs_dir()
        src_key = flow_keys[0]
        local = flows_dir / os.path.basename(src_key)

        if not local.exists():
            _download(BUCKET, src_key, str(local))

        return local

    # For the existing filename search
    def _find_any_owp_for_return_period(
        self, huc8: str, return_period: int
    ) -> Optional[Path]:
        """
        Look inside the inundation folder for a BLE return-period inundation tif.
        Match any tif whose name contains both the HUC8 and {return_period}YR tokens.
        e.g. BLE_HUC_11110205_FLOWS_100YR_921950W351837N_inundation.tif
        """
        dirp = self.owp_root / f"flood_{huc8}" / f"{huc8}_inundation"
        if not dirp.exists():
            return None
        rp_tag = f"{return_period}YR"
        cand = [
            p
            for p in dirp.glob("BLE*_inundation.tif")
            if huc8 in p.name and rp_tag in p.name.upper()
        ]
        if not cand:
            return None
        return sorted(cand, key=lambda p: p.stat().st_mtime, reverse=True)[0]

    def _generate_owp_return_period(
        self,
        huc8: str,
        return_period: int,
        dest_dirs: List[str],
    ) -> Optional[str]:
        self._ensure_roots()

        rp = int(return_period)

        # If already generated, just copy to dest dirs -- same logic as the date-based path
        existing = self._find_any_owp_for_return_period(huc8, rp)
        if existing and existing.exists():
            copied_any: Optional[str] = None
            for d in dest_dirs:
                copied_any = self._copy_to_dest(existing, d)
            return copied_any

        print(f"Generating return-period HAND FIM for HUC {huc8} (RP={rp})...")
        DownloadHUC8(huc8, version="4.8")

        flows_csv = self._download_return_period_flows_csv(huc8, rp)
        print(f"Downloaded return-period flows CSV to '{flows_csv}'.")

        runOWPHANDFIM(huc8)

        # After run, re-check the inundation folder for the produced tif
        produced = self._find_any_owp_for_return_period(huc8, rp)
        if not produced:
            return None

        # Copy produced tif into each destination folder
        copied_any: Optional[str] = None
        for d in dest_dirs:
            copied_any = self._copy_to_dest(produced, d)

        return copied_any

    # HWM Range helpers
    def _expected_owp_path_hwm(self, huc8: str, start: str, end: str) -> Path:
        """
        Naming convention for HWM range-based maximum discharge maps.
        Format: NWM_STARTDATE_ENDDATE_maximum_HUC_inundation.tif
        """
        s, e = start.replace("-", ""), end.replace("-", "")
        name = f"NWM_{s}_{e}_maximum_{huc8}_inundation.tif"
        return self.owp_root / f"flood_{huc8}" / f"{huc8}_inundation" / name

    # Query
    def query(
        self,
        HUCID: str,
        date_input: Optional[str] = None,
        file_name: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        return_period: Optional[int] = None,
    ) -> Dict[str, Any]:
        catalog = load_catalog_core()
        records = catalog.get("records", [])
        huc8 = str(HUCID).strip()

        # strict set
        strict_matches = find_fims(
            records,
            huc8=huc8,
            date_input=date_input,
            file_name=file_name,
            start_date=start_date,
            end_date=end_date,
            return_period=return_period,
            relaxed_for_print=False,
        )

        # relaxed set
        relaxed_matches = find_fims(
            records,
            huc8=huc8,
            date_input=date_input,
            file_name=file_name,
            start_date=start_date,
            end_date=end_date,
            return_period=return_period,
            relaxed_for_print=True,
        )

        status = (
            "ok"
            if strict_matches
            else (
                "info"
                if (
                    date_input is None
                    and file_name is None
                    and not start_date
                    and not end_date
                )
                else "not_found"
            )
        )

        base_msg = f"Found {len(strict_matches)} record(s) for HUC {huc8}"
        if date_input:
            base_msg += f" and '{date_input}'"
        if file_name:
            base_msg += f" and file '{file_name}'"
        if start_date or end_date:
            base_msg += f" in range [{start_date or '-∞'} , {end_date or '∞'}]"
        
        printable = format_records_for_print(relaxed_matches)
        return {
            "status": status,
            "message": base_msg + "\n" + self.availability(huc8),
            "matches": strict_matches,
            "printable": printable,
        }

    # Trigger the process to download the benchmark FIM data and ensure/generate the OWP HAND FIM
    def process(
        self,
        huc8: str,
        date_input: Optional[str] = None,
        ensure_owp: bool = True,
        generate_owp_if_missing: bool = True,
        out_dir: Optional[str] = None,
        file_name: Optional[str] = None,
        return_period: Optional[int] = None,
        start_date: Optional[str] = None,  
        end_date: Optional[str] = None,  
    ) -> Dict[str, Any]:
        catalog = load_catalog_core()
        records = catalog.get("records", [])
        strict_matches = find_fims(
            records,
            huc8=str(huc8).strip(),
            date_input=date_input,
            file_name=file_name,
            start_date=start_date,
            end_date=end_date,
            return_period=return_period,
            relaxed_for_print=False,
        )
        if out_dir:
            inputs_root = Path(out_dir)
        else:
            inputs_root = Path(os.getcwd()) / "FIMevaluation_inputs"
            inputs_root.mkdir(parents=True, exist_ok=True)

        self._ensure_roots()

        # If strict match missing but filename given --> fallback to filename-based lookup
        if not strict_matches:
            if file_name:
                fname = file_name.strip()
                cand_same_huc = [
                    r
                    for r in records
                    if str(r.get("file_name", "")).strip() == fname
                    and str(huc8).strip() in set(_record_huc8_list(r))
                ]
                cand_any_huc = [
                    r for r in records if str(r.get("file_name", "")).strip() == fname
                ]
                rec = (
                    cand_same_huc[0]
                    if cand_same_huc
                    else (cand_any_huc[0] if cand_any_huc else None)
                )

                if rec is None:
                    msg = (
                        f"No strict benchmark match for HUC {huc8}"
                        + (f" and '{date_input}'" if date_input else "")
                        + f", and file '{file_name}' not found in catalog."
                    )
                    return {
                        "status": "not_found",
                        "message": msg,
                        "folders": [],
                        "matches": [],
                    }

                site = self._site_of(rec)
                folder = inputs_root / f"HUC{huc8}_{site}"
                folder.mkdir(parents=True, exist_ok=True)

                dl = download_fim_assets(
                    rec, str(folder), return_period=None, download_flows=False
                )

                owp_path = None
                if ensure_owp:
                    # Check if the assumed file record has range metadata
                    rec_start = rec.get("start_date_ymd")
                    rec_end = rec.get("end_date_ymd")

                    owp_path = self._ensure_owp_to(
                        huc8=huc8,
                        user_dt=date_input,
                        dest_dir=str(folder),
                        generate_if_missing=generate_owp_if_missing,
                        return_period=return_period,
                        start_date=rec_start or start_date,
                        end_date=rec_end or end_date
                    )

                msg = (
                    f"Used user-specified file '{file_name}' as benchmark reference. "
                    f"Downloaded into '{folder}'."
                )
                return {
                    "status": "assumed",
                    "message": msg,
                    "folders": [
                        {
                            "label": site,
                            "folder": str(folder),
                            "records": [rec],
                            "downloads": [{"record": rec, "downloads": dl}],
                            "owp_path": owp_path,
                        }
                    ],
                    "matches": [rec],
                }

            msg = f"No strict benchmark match for HUC {huc8}" + (
                f" and '{date_input}'" if date_input else ""
            )
            return {"status": "not_found", "message": msg, "folders": [], "matches": []}

        # Group records by their event label
        label_map: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
        for rec in strict_matches:
            label = (
                self._date_label_from_user(date_input)
                if date_input
                else self._date_label_for_record(rec)
            )
            label_map[label].append(rec)

        folders_out: List[Dict[str, Any]] = []
        total_downloaded = 0
        ran_return_period = False

        for label, recs in sorted(label_map.items()):
            dl_by_site: Dict[str, List[Dict[str, Any]]] = {}
            for rec in recs:
                site = self._site_of(rec)
                folder = inputs_root / f"HUC{huc8}_{site}"
                folder.mkdir(parents=True, exist_ok=True)

                dl = download_fim_assets(
                    rec, str(folder), return_period=None, download_flows=False
                )
                dl_by_site.setdefault(site, []).append({"record": rec, "downloads": dl})
                if dl.get("tif") or dl.get("gpkg_files"):
                    total_downloaded += 1

            # Identify if this event is an HWM Range based on the first record
            current_rec = recs[0]
            rec_start = current_rec.get("start_date_ymd")
            rec_end = current_rec.get("end_date_ymd")

            user_dt = self._user_dt_from_label(label)

            if ensure_owp and (return_period is not None) and (not ran_return_period):
                dest_dirs = [
                    str(inputs_root / f"HUC{huc8}_{site}") for site in dl_by_site
                ]
                self._generate_owp_return_period(huc8, int(return_period), dest_dirs)
                ran_return_period = True

            owp_src_copied_any = False
            if ensure_owp:
                for site in dl_by_site:
                    folder = inputs_root / f"HUC{huc8}_{site}"
                    owp_path = self._ensure_owp_to(
                        huc8=huc8,
                        user_dt=user_dt,
                        dest_dir=str(folder),
                        generate_if_missing=generate_owp_if_missing,
                        return_period=return_period,
                        start_date=rec_start or start_date,
                        end_date=rec_end or end_date
                    )
                    if owp_path:
                        owp_src_copied_any = True

            for site, dl_records in dl_by_site.items():
                folder = inputs_root / f"HUC{huc8}_{site}"
                
                # Dynamic pathing for the return dictionary
                if rec_start and rec_end:
                    expected_name = self._expected_owp_path_hwm(huc8, rec_start, rec_end).name
                    final_owp_path = str(folder / expected_name) if owp_src_copied_any else None
                else:
                    final_owp_path = str(folder / f"NWM_{label}_{huc8}_inundation.tif") if (ensure_owp and owp_src_copied_any) else None

                folders_out.append(
                    {
                        "label": site if date_input else f"{label}:{site}",
                        "folder": str(folder),
                        "records": [d["record"] for d in dl_records],
                        "downloads": dl_records,
                        "owp_path": final_owp_path,
                    }
                )

        # Build message
        msg_bits = [f"Downloaded {total_downloaded} benchmark item(s) into '{inputs_root}'."]
        if ensure_owp:
            if return_period is not None:
                msg_bits.append(f"OWP HAND FIM generated for {return_period} year return period.")
            elif rec_start and rec_end:
                msg_bits.append(f"OWP HAND FIM generated using MAXIMUM discharge for range {rec_start} to {rec_end}.")
            else:
                msg_bits.append("OWP HAND FIMs ensured per event.")

        return {
            "status": "ok",
            "message": " ".join(msg_bits),
            "folders": folders_out,
            "matches": strict_matches,
        }

    # Internals
    @staticmethod
    def _date_label_for_record(rec: Dict[str, Any]) -> str:
        day = _record_day(rec)
        hh = _record_hour_or_none(rec)
        if not day:
            raw = str(rec.get("date_raw", ""))[:8]
            return raw if raw else "unknown"
        if hh is None:
            return f"{day:%Y%m%d}"
        return f"{day:%Y%m%d}{hh:02d}0000"

    @staticmethod
    def _date_label_from_user(user_dt: str) -> str:
        day = _to_date(user_dt)
        hh = _to_hour_or_none(user_dt)
        return f"{day:%Y%m%d}" if hh is None else f"{day:%Y%m%d}{hh:02d}0000"

    def _user_dt_from_label(self, label: str) -> Optional[str]:
        """Convert a YYYYMMDD string back into a dash-separated user string."""
        try:
            if len(label) == 8:
                return f"{label[:4]}-{label[4:6]}-{label[6:]}"
            elif len(label) >= 10:
                return f"{label[:4]}-{label[4:6]}-{label[6:8]}T{label[8:10]}"
        except:
            pass
        return None

    def _ensure_owp_to(
        self,
        huc8: str,
        user_dt: Optional[str],
        dest_dir: str,
        generate_if_missing: bool,
        return_period: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Optional[str]:
        """
        Idempotent ensure:
        - Checks for HWM Range maps first.
        - Checks for specific timestamps or daily maps.
        """
        # HWM Range Map
        if start_date and end_date:
            existing = self._expected_owp_path_hwm(huc8, start_date, end_date)
            if existing.exists():
                return self._copy_to_dest(existing, dest_dir)
            
            if generate_if_missing:
                produced = self._generate_owp(
                    huc8=huc8, user_dt=None, return_period=return_period, 
                    start_date=start_date, end_date=end_date
                )
                if produced and produced.exists():
                    return self._copy_to_dest(produced, dest_dir)
            return None

        # Standard Day/Hour Map
        if not user_dt:
            return None

        ymd, timestr = self._ymd_timestr_from_user(user_dt)

        if timestr is None:
            existing = self._find_any_owp_for_day(huc8, ymd)
        else:
            ep = self._expected_owp_path(huc8, ymd, timestr)
            existing = ep if ep.exists() else None

        if existing and existing.exists():
            return self._copy_to_dest(existing, dest_dir)

        if generate_if_missing:
            produced = self._generate_owp(huc8, user_dt, return_period=return_period)
            if produced and produced.exists():
                return self._copy_to_dest(produced, dest_dir)

        return None

    # OWP helpers
    def _ymd_timestr_from_user(self, user_dt: str) -> Tuple[str, Optional[str]]:
        day = _to_date(user_dt)
        hh = _to_hour_or_none(user_dt)
        ymd = day.strftime("%Y%m%d")
        return (ymd, None) if hh is None else (ymd, f"{hh:02d}0000")

    def _expected_owp_path(self, huc8: str, ymd: str, timestr: Optional[str]) -> Path:
        name = (
            f"NWM_{ymd}_{huc8}_inundation.tif"
            if timestr is None
            else f"NWM_{ymd}{timestr}_{huc8}_inundation.tif"
        )
        return self.owp_root / f"flood_{huc8}" / f"{huc8}_inundation" / name

    @staticmethod
    def _copy_to_dest(src: Path, dest_dir: str) -> str:
        dst_dir = Path(dest_dir)
        dst_dir.mkdir(parents=True, exist_ok=True)
        dst = dst_dir / src.name
        shutil.copy2(src, dst)
        return str(dst)

    def _generate_owp(
        self, 
        huc8: str, 
        user_dt: Optional[str], 
        return_period: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[Path]:
        """
        Main generation driver.
        - Supports HWM ranges with Maximum Discharge sort.
        - Supports return periods.
        - Supports standard event points.
        """
        self._ensure_roots()

        # Check existing first to avoid double processing
        if start_date and end_date:
            expected = self._expected_owp_path_hwm(huc8, start_date, end_date)
            if expected.exists(): return expected
        elif user_dt:
            ymd, timestr = self._ymd_timestr_from_user(user_dt)
            expected = self._expected_owp_path(huc8, ymd, timestr)
            if expected.exists(): return expected

        print(f"Generating OWP HAND FIM for HUC {huc8}...")
        DownloadHUC8(huc8, version="4.8")

        # HWM Range --> Maximum Discharge over the date range
        if start_date and end_date:
            print(f"Triggering NWM retrospective for range {start_date} to {end_date} (Sort: Maximum)")
            getNWMretrospectivedata(
                huc=str(huc8), 
                start_date=start_date, 
                end_date=end_date, 
                discharge_sortby="maximum"
            )
            runOWPHANDFIM(huc8)
            return self._expected_owp_path_hwm(huc8, start_date, end_date)

        # Return Period (BLE)--> get the flows from aws
        if return_period is not None:
            self._download_return_period_flows_csv(huc8, int(return_period))
            runOWPHANDFIM(huc8)
            return self._find_any_owp_for_return_period(huc8, int(return_period))

        # Standard specific event --> using the date input
        if user_dt:
            day = _to_date(user_dt)
            hh = _to_hour_or_none(user_dt)
            stamp = f"{day:%Y-%m-%d}" if hh is None else f"{day:%Y-%m-%d} {hh:02d}:00:00"
            getNWMretrospectivedata(huc_event_dict={str(huc8): [stamp]})
            runOWPHANDFIM(huc8)
            ymd, timestr = self._ymd_timestr_from_user(user_dt)
            return self._expected_owp_path(huc8, ymd, timestr)
        
        return None
    
#Main wrapper
def fim_lookup(
    HUCID: str,
    date_input: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    return_period: Optional[int] = None,
    file_name: Optional[str] = None,
    run_handfim: bool = False,
    out_dir: Optional[str] = None,
) -> str:
    """
    Simplified wrapper to interact with FIMService.
    - If run_handfim is True: It triggers 'process' (Downloads + OWP Generation).
    - If run_handfim is False: It triggers 'query' (Metadata listing).
    """
    svc = FIMService()

    # Filename provided OR run_handfim is True
    if file_name or run_handfim:
        rep = svc.process(
            huc8=HUCID,
            date_input=date_input,
            ensure_owp=run_handfim,
            generate_owp_if_missing=run_handfim,
            out_dir=out_dir,
            file_name=file_name,
            return_period=return_period,
            start_date=start_date, 
            end_date=end_date 
        )
        return rep.get("message", "")

    # Just looking for what is available --> QUERY Mode
    q = svc.query(
        HUCID=HUCID,
        date_input=date_input,
        file_name=None,
        start_date=start_date,
        end_date=end_date,
        return_period=return_period,
    )
    
    txt = q.get("printable") or ""
    if not txt.strip():
        return (
            f"No benchmark FIMs matched for HUC {HUCID}. "
            f"Range: [{start_date or '-∞'} , {end_date or '∞'}]"
        )

    header = "Following are the available benchmark data"
    filt = [f"HUC {HUCID}"]
    if date_input: filt.append(f"date '{date_input}'")
    if start_date or end_date: filt.append(f"range [{start_date or '-∞'} , {end_date or '∞'}]")
    
    return f"{header} for {', '.join(filt)}:\n{txt}"