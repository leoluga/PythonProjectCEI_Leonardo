from typing import Optional, Union 
import dash_ag_grid as dag 
import pandas as pd 

STATS_ROW_STYLE = { 
    "styleConditions": [ 
        { 
            "condition": "params.node.rowIndex == 0 | params.node.rowIndex == 5", 
            "style": {"backgroundColor": "#555555", "color": "white"}, 
        }, 
            
    ] 
} 

def make_ag_grid( 
    table_id: str, 
    main_dict: dict, 
    width: Optional[Union[str, int]] = "100%", 
    height: Optional[Union[str, int]] = "100%", 
    float_filter=False, 
    dom_layout="autoHeight", 
    wrap_header=False, 
    ag_grid_paginated=False, 
    page_size=35, 
    editable = False, 
    row_heigth = None 
) -> dag.AgGrid: 
    """ Helper function that requires a dict with the df, columns definitions and row styles, 
    to create a dag.AgGrid component. 
    
    Args: table_id (str): description main_dict (dict): description width (Optional[Union[str, int]], optional): description. Defaults to "100%". height (Optional[Union[str, int]], optional): description. Defaults to "100%". float_filter (bool, optional): description. Defaults to False. dom_layout (str, optional): description. Defaults to "autoHeight". Returns: dag.AgGrid: description """ 
    
    for col in main_dict["df"].columns[main_dict["df"].dtypes == "datetime64[ns]"]: 
        main_dict["df"][col] = main_dict["df"][col].dt.strftime("%Y-%m-%d") 
    
    default_col_def = { 
        "filter": True, 
        "resizable": True, 
        "sortable": True, 
        "editable": editable, 
        "floatingFilter": float_filter, 
        "wrapHeaderText": wrap_header, 
        "autoHeaderHeight": wrap_header, 
        "cellStyle": { 
            "styleConditions": [ 
                { 
                    "condition": "params.value < 0", 
                    "style": {"color": "red", "font-weight": "700"}, 
                }, 
            ], 
        }, 
    } 
    
    dash_ag_grid_options = { 
        "rowSelection": "multiple", 
        "suppressAggFuncInHeader": True, 
        "domLayout": dom_layout, 
        "enableFilter": True, 
        "enableRangeSelection": True, 
        "suppressHorizontalScroll": True, 
        "groupDefaultExpanded": 1, 
        "rowHeight": row_heigth } 
    
    if ag_grid_paginated: 
        dash_ag_grid_options["pagination"] = True 
        dash_ag_grid_options["paginationPageSize"] = page_size 
    
    grid = dag.AgGrid( 
        id=table_id, 
        className="ag-theme-balham-dark", 
        columnDefs=main_dict["col_def"], 
        enableEnterpriseModules=True, 
        rowData=main_dict["df"].reset_index().to_dict("records"), 
        getRowStyle=main_dict["row_style"], 
        defaultColDef=default_col_def, 
        columnSize="responsiveSizeToFit", 
        persistence=True, 
        persisted_props=["filterModel"], 
        dashGridOptions=dash_ag_grid_options, 
        style={"width": width, "height": height},
    )
    
    return grid