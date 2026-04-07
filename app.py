import streamlit as st
import pandas as pd
import asyncio
import re
import aiohttp
import json
from typing import Dict, Any, List

st.set_page_config(
    page_title="Cricket Intelligence Hub",
    layout="wide",
    initial_sidebar_state="expanded",
)

IMAGE_BASE_URL = "https://img1.hscicdn.com/image/upload"

st.markdown(
    """
    <style>
    .stApp { background-color: #f8fafc; }
    .match-card {
        background-color: white;
        padding: 1.5rem;
        border-radius: 12px;
        border: 1px solid #e2e8f0;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
        margin-bottom: 1rem;
    }
    div[data-testid="stMetric"] {
        background-color: white;
        border: 1px solid #e2e8f0;
        padding: 15px;
        border-radius: 10px;
    }
    .stButton button {
        border-radius: 8px;
        font-weight: 600;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

def get_full_image_url(path: str):
    if not path: return None
    clean_path = path.lstrip('/')
    return f"{IMAGE_BASE_URL}/{clean_path}" if not path.startswith("http") else path

def extract_series_id(url: str):
    pattern = r"series/[\w-]+-(\d+)"
    match = re.search(pattern, url)
    return match.group(1) if match else None

def process_flat_schema(raw_data_list):
    if not raw_data_list: return pd.DataFrame(), {}
    df = pd.json_normalize(raw_data_list, sep=".")
    df = df.loc[:, ~df.columns.duplicated()]
    hierarchy = {}
    for col in df.columns:
        parent = col.split(".")[0] if "." in col else "Base"
        if parent not in hierarchy: hierarchy[parent] = []
        hierarchy[parent].append(col)
    return df, hierarchy

async def fetch_series_matches(series_id: int):
    from apis import AsyncSeriesClient
    async with aiohttp.ClientSession() as session:
        client = AsyncSeriesClient(session=session)
        res = await client.get_schedule(id=series_id)
        results = res.get("content", {}).get("matches", [])
        return [{
            "id": m.get("objectId"),
            "label": f"{m.get('title')}: {m.get('teams')[0]['team']['abbreviation']} v {m.get('teams')[1]['team']['abbreviation']}",
            "status": m.get("status"),
            "outcome": m.get("statusText"),
            "venue": m.get("ground", {}).get("name"),
            "t1_logo": get_full_image_url(m.get('teams')[0]['team'].get("imageUrl")),
            "t2_logo": get_full_image_url(m.get('teams')[1]['team'].get("imageUrl")),
        } for m in results]

async def fetch_ball_by_ball(series_id: int, match_id: int, session: aiohttp.ClientSession):
    from apis import AsyncMatchClient
    client = AsyncMatchClient(session=session)
    comm = await client.get_commentary(id=match_id, series_id=series_id) or {}
    m_meta = comm.get("match", {})
    total_innings = int(m_meta.get("liveInning") or (2 if m_meta.get("status") == "RESULT" else 0))
    match_deliveries = []
    for inn in range(1, total_innings + 1):
        frm = 1
        while True:
            res = await client.get_ball_commentary(id=match_id, series_id=series_id, inning_number=inn, from_over=frm)
            comments = (res or {}).get("comments") or []
            if not comments: break
            for c in comments:
                c["match_context.id"], c["match_context.inning"] = match_id, inn
            match_deliveries.extend(comments)
            frm = (res or {}).get("nextInningOver")
            if not frm or frm == -1: break
    return match_deliveries

async def batch_extract(series_id: int, match_ids: List[int]):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_ball_by_ball(series_id, mid, session) for mid in match_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return [ball for match in results if isinstance(match, list) for ball in match]

def main():
    if "last_url" not in st.session_state: st.session_state["last_url"] = ""
    if "loaded_config" not in st.session_state: st.session_state["loaded_config"] = {}

    with st.sidebar:
        st.markdown("### Data Ingestion")
        url = st.text_input("Series URL", value="https://www.espncricinfo.com/series/ipl-2026-1510719")
        
        if url != st.session_state["last_url"]:
            st.session_state["last_url"] = url
            for key in ["fixtures", "raw_data"]:
                if key in st.session_state: del st.session_state[key]

        s_id = extract_series_id(url)
        if s_id:
            st.success(f"Detected Series ID: {s_id}")
            if st.button("Sync Match Schedule", use_container_width=True):
                st.session_state["fixtures"] = asyncio.run(fetch_series_matches(int(s_id)))
                st.session_state["s_id"] = s_id
        
        if "fixtures" in st.session_state:
            st.divider()
            st.markdown("### Extraction Queue")
            completed = [m for m in st.session_state["fixtures"] if m["status"] == "RESULT"]
            select_all = st.checkbox("Select All Completed", value=True)
            selected_ids = [m["id"] for m in completed if st.checkbox(m["label"], key=f"q_{m['id']}", value=select_all)]

            if st.button("Download Ball Data", type="primary", use_container_width=True, disabled=not selected_ids):
                with st.spinner("Extracting..."):
                    st.session_state["raw_data"] = asyncio.run(batch_extract(int(st.session_state["s_id"]), selected_ids))
                    st.toast("Extraction Complete!")

        st.divider()
        st.markdown("### Configuration Management")
        uploaded_file = st.file_uploader("Load Mapper JSON", type=["json"])
        if uploaded_file is not None:
            try:
                st.session_state["loaded_config"] = json.load(uploaded_file)
                st.success("Config Loaded")
            except Exception as e:
                st.error(f"Error: {e}")

    if "fixtures" in st.session_state:
        tab_fixtures, tab_workspace = st.tabs(["Fixture Inventory", "Dataset Architect"])

        with tab_fixtures:
            col_list, col_info = st.columns([2, 1])
            with col_list:
                for m in st.session_state["fixtures"]:
                    st.markdown(f"""
                    <div class="match-card">
                        <div style="display: flex; align-items: center; justify-content: space-between;">
                            <div style="display: flex; align-items: center; gap: 15px;">
                                <img src="{m['t1_logo']}" width="40">
                                <span style="font-weight: 700; font-size: 1.1rem; color: #1e293b;">{m['label']}</span>
                                <img src="{m['t2_logo']}" width="40">
                            </div>
                            <span style="font-size: 0.75rem; font-weight: 800; color: {'#10b981' if m['status'] == 'RESULT' else '#f59e0b'}; background: {'#ecfdf5' if m['status'] == 'RESULT' else '#fffbeb'}; padding: 4px 10px; border-radius: 20px; border: 1px solid;">
                                {m['status'] if m['status'] == 'RESULT' else 'UPCOMING'}
                            </span>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
            with col_info:
                st.info("Load a JSON config to restore field mappings")

        with tab_workspace:
            if "raw_data" in st.session_state:
                df_flat, hierarchy = process_flat_schema(st.session_state["raw_data"])
                
                L, R = st.columns([1, 2])
                with L:
                    st.markdown("#### 1. Schema Definition")
                    
                    default_parents = [p for p in hierarchy.keys() if any(k.startswith(p) for k in st.session_state["loaded_config"].keys())]
                    
                    selected_parents = st.multiselect(
                        "Data Objects", 
                        options=sorted(hierarchy.keys()),
                        default=default_parents if default_parents else None
                    )
                    
                    final_columns = []
                    for p in selected_parents:
                        with st.expander(f"Group: {p}", expanded=True):
                            default_attrs = [a for a in hierarchy[p] if a in st.session_state["loaded_config"].keys()]
                            
                            chosen = st.multiselect(
                                f"Fields in {p}", 
                                options=hierarchy[p], 
                                key=f"p_{p}",
                                default=default_attrs if default_attrs else None
                            )
                            final_columns.extend(chosen)

                with R:
                    st.markdown("#### 2. CSV Header Mapping")
                    if final_columns:
                        mapping = {}
                        grid = st.columns(2)
                        for i, f in enumerate(final_columns):
                            with grid[i % 2]:
                                default_val = st.session_state["loaded_config"].get(f, f.split(".")[-1])
                                mapping[f] = st.text_input(f"Path: {f}", value=default_val, key=f"map_{f}")

                        final_mapping, used = {}, set()
                        for path, req in mapping.items():
                            name, count = req, 1
                            while name in used:
                                name = f"{req}_{count}"; count += 1
                            final_mapping[path] = name; used.add(name)

                        export_df = df_flat[final_columns].rename(columns=final_mapping)
                        st.dataframe(export_df.head(50), use_container_width=True)
                        
                        c_down1, c_down2 = st.columns(2)
                        with c_down1:
                            st.download_button(
                                "Download CSV", 
                                export_df.to_csv(index=False).encode("utf-8"), 
                                file_name=f"data_{s_id}.csv", 
                                type="primary", 
                                use_container_width=True
                            )
                        with c_down2:
                            config_json = json.dumps(mapping, indent=4)
                            st.download_button(
                                "Save Config JSON",
                                data=config_json,
                                file_name="config.json",
                                mime="application/json",
                                use_container_width=True
                            )
            else:
                st.warning("Extract match data to begin architecture")
    else:
        st.write("Initialize Series URL to proceed")

if __name__ == "__main__":
    main()
