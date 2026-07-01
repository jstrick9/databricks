import pandas as pd
import io
import streamlit as st

# --- PRESET CODES LIST ---
PRESET_ACTION_CODES = sorted([
    "1.AA", "1.AD", "1.BA", "1.BD", "12AB", "12AF", "15AB", "15AF", "15AK", "15IA",
    "44DA", "44DD", "44EA", "44ED", "44EG", "44EM", "44EP", "7.PR", "71.P", "715F",
    "71AF", "71AG", "8.AF", "8.OK", "8.PR", "806I", "810I", "815F", "815I", "89AF",
    "89AG", "89IA", "8AFT", "8OKT", "8PRT", "9.AF", "9G8P", "9INF", "AAUA", "AAUD",
    "AAUW", "ABN0", "ABN1", "ABN2", "ABN3", "ABN4", "ABN5", "ABN6", "ABN7", "ABN8",
    "ABN9", "ABND", "ACEC", "ACFC", "ADBS", "ADCH", "AITU", "ALIE", "AMD7", "AMPB",
    "AMPR", "AMPX", "AOUM", "APRC", "APRE", "APET", "ARAA", "ASCK", "ASDF", "ASGN",
    "ATAP", "ATDN", "ATRQ", "ATRV", "AUPC", "AORN", "B7MD", "BARR", "BARX", "BDRD",
    "BDRN", "BDXD", "BDXN", "BNTR", "BNTX", "BPCQ", "BPET", "BPPR", "BPRE", "BPXR",
    "BPXX", "BR3R", "BRBF", "BRFA", "BRNT", "BRPI", "BSRF", "BURX", "BX3R", "BXFA",
    "BXNT", "BXOA", "BXPI", "C.7C", "C.7F", "C15A", "C15P", "C18.", "C18P", "C24.",
    "C37.", "C37P", "C6AF", "C6AP", "C6BF", "C6BP", "C7..", "C71P", "C71T", "C75A",
    "C75P", "C7P.", "C7PF", "C7RF", "C8..", "C8.T", "C8P.", "CAEX", "CAND", "CANG",
    "CANT", "CCCN", "CCON", "CEAP", "CEPE", "CFRC", "CFDD", "CFDG", "CFDM", "CFIT",
    "CHAN", "CHLD", "CHPB", "CHPN", "CIID", "CIIG", "CIIM", "CMMP", "CNCF", "CNEA",
    "CNES", "CNFR", "CNPR", "CNRT", "CNRU", "CNSA", "CNSI", "CNSL", "CNSR", "CNTA",
    "COC.", "COCO", "COAE", "COAR", "CORN", "CORR", "CORS", "CORV", "CPEA", "CPRA",
    "CRAF", "CRCR", "CRCV", "CREV", "CRFA", "CRML", "CRNO", "CRRC", "CRRR", "CRSN",
    "CRST", "CRTP", "CTDA", "CTDD", "CTDR", "CTDV", "CTRD", "CU.A", "CU.D", "CU.G",
    "CU.I", "CU.M", "CU.T", "CWBI", "CWBP", "D1BN", "D1BR", "DBCR", "DBCS", "DCPN",
    "DENA", "DENC", "DETH", "DIPR", "DIPX", "DMCC", "DNLR", "DOCK", "DP1B", "DPCC",
    "DRHI", "DRRR", "DVUT", "E15R", "E815", "E89R", "EAAU", "EADG", "EARE", "EARS",
    "ECCA", "ECDR", "EEXT", "EISU", "EMRV", "ENOR", "EPEN", "EPGS", "EPPA", "EPRR",
    "ERCF", "ERFR", "ERFT", "EROI", "EROP", "ERRR", "ERRS", "ERSI", "ERTD", "ERTI",
    "ERTP", "ERTR", "ES71", "ES75", "ES7R", "ES7S", "ES8R", "ETOF", "ETOP", "EWAF",
    "EWOR", "EX1D", "EX1G", "EX1M", "EX2D", "EX2G", "EX2M", "EX3D", "EX3G", "EX3M",
    "EX4D", "EX4G", "EX4M", "EX5D", "EX5G", "EX5M", "EXAB", "EXAF", "EXAR", "EXDA",
    "EXDD", "EXDM", "EXDR", "EXFB", "EXNI", "EXPA", "EXPI", "EXPR", "EXPT", "EXRA",
    "EXRE", "EXRG", "EXRR", "EXT1", "EXT2", "EXT3", "EXT4", "EXT5", "FAXX", "FBCS",
    "FBNX", "FDNP", "FDNT", "FDPP", "FDSP", "FFDM", "FICR", "FICS", "FIMP", "FINA",
    "FINO", "FINP", "FINT", "FINV", "FISN", "FIXD", "GAUD", "GAUN", "GEA1", "GEA2",
    "GEAN", "GEAP", "GECG", "GECD", "GNCF", "GNEA", "GNEN", "GNES", "GNFN", "GNFR",
    "GNR1", "GNRN", "GNRT", "GNRU", "GNS1", "GNS2", "GNS3", "GNSF", "GNSI", "GNSL",
    "GNSN", "GNPA", "GNPE", "GPAR", "GPAS", "GPNX", "GPRA", "GPRN", "GRDA", "GRML",
    "GSEA", "GSEN", "GSPR", "GSPS", "GSSF", "GSS1", "HSCD", "ICNA", "IIRN", "IIOA",
    "INCD", "INCE", "INCS", "INNA", "INNP", "INNT", "INOA", "INPC", "INPR", "INPS",
    "INTI", "INTR", "INTS", "INTT", "IRCB", "IRFI", "IRGP", "IRIV", "IRRE", "IRRF",
    "IRRG", "IRRH", "IROP", "IROT", "IRSS", "IRXX", "ISCO", "ISCR", "ISIC", "ISER",
    "ISIR", "ISIU", "ISMR", "ISPO", "ISPR", "ISSR", "ISTB", "ISTQ", "ISUR", "IUAA",
    "IUAF", "IUAL", "IUCN", "IUFF", "IULN", "IUPC", "IURF", "IUSF", "JURT", "KBOC",
    "KNPR", "KNPRO", "KNPRP", "KNSC", "KNOT", "KOFS", "KONO", "KORN", "KORR", "KRAR",
    "KRCC", "KRNT", "KRRC", "KRRE", "KRRT", "KRSC", "KSAD", "KSCO", "KSCOO", "KSCOP",
    "KSDR", "KSNR", "KSNS", "L70", "L90", "LEXT", "LIEC", "LIEN", "LIME", "LIMG",
    "LIMI", "LIMN", "LIMS", "LNAR", "LNAS", "LNNX", "LOAP", "LOPE", "LOPR", "LOPT",
    "LSOU", "M70", "MAB0", "MAB1", "MAB2", "MAB3", "MAB4", "MAB5", "MAB6", "MAB7",
    "MAB8", "MAFR", "MAIL", "MDSC", "MDSM", "MNDA", "MPMK", "MREI", "MSNI", "N12C",
    "N6AF", "NA15", "NA71", "NA75", "NA85", "NA89", "NAAX", "NAS8", "NAUD", "NC71",
    "NCP7", "NCS7", "NCS8", "NEWN", "NEWR", "NIAP", "NOAC", "NOAD", "NOAM", "NONP",
    "NOPM", "NP89", "NPUB", "NRCC", "NRCS", "NREN", "NREP", "NREV", "NURC", "NWAP",
    "NWOS", "OHER", "OHMR", "OHPO", "OHSR", "OP.D", "OP.I", "OP.N", "OP.S", "OP.T",
    "OP2R", "OP2S", "OPNC", "OPNR", "OPNS", "OPNX", "OPPF", "OQ89", "ORDR", "OROR",
    "OTHE", "P12", "P12C", "P13", "PAIS", "PARI", "PBCO", "PBCR", "PBER", "PBIC",
    "PBIR", "PBMR", "PBPO", "PBPR", "PBSR", "PBTB", "PBTQ", "PBUR", "PC.D", "PCBD",
    "PCBG", "PCBM", "PCDE", "PCGR", "PCNX", "PCPN", "PCRC", "PDCB", "PDMS", "PDPS",
    "PDTG", "PDTR", "PDWF", "PETC", "PETD", "PETG", "PETI", "PETL", "PETR", "PG1B",
    "PGDV", "PGEX", "PGOA", "PGRN", "PGRR", "PGSU", "PGTT", "PILM", "PINM", "PINT",
    "PIRC", "PLGL", "PMSG", "PMSD", "PMSM", "PPAC", "PPAD", "PPAR", "PPCD", "PR.D",
    "PR.W", "PR12", "PR15", "PR23", "PR71", "PR75", "PR89", "PRA7", "PRA8", "PRA9",
    "PRAM", "PRAN", "PREV", "PRIM", "PRIC", "PRMP", "PROA", "PRPC", "PRRD", "PRRG",
    "PRRR", "PUBO", "PUBW", "PUM1", "PUM2", "PUM3", "PUMI", "PUNQ", "PUNR", "PWFD",
    "PWFG", "PWFM", "QR15", "QR23", "QR71", "QR75", "QRA8", "QRAM", "R.PR", "R.SR",
    "R70", "R90", "RAPP", "RBFT", "RCCK", "RCFR", "RCII", "RCPN", "RCSC", "RDEN",
    "RDNY", "RDX1", "RDX2", "RDX3", "REAP", "RECD", "RECG", "REGV", "REIN", "REM1",
    "REM2", "REM3", "REM4", "REN1", "REN2", "REN3", "REN4", "REN5", "REPR", "RETP",
    "RFCR", "RFCS", "RFIL", "RFNP", "RFNT", "RFRC", "RFRR", "RFSH", "RFTP", "RFWR",
    "RG1B", "RGDV", "RGEX", "RGIA", "RGLA", "RGOA", "RGRN", "RGRR", "RGSU", "RGTR",
    "RGTT", "RGTD", "RHRD", "RINX", "RMDT", "RMRF", "RNL1", "RNL2", "RNL3", "RNL4",
    "RNL5", "RNL6", "RNL7", "RNL8", "RNL9", "RNWL", "RPRI", "RPRC", "RPUB", "RRDX",
    "RRGD", "RRGG", "RRGM", "RRPR", "RSHD", "RSHG", "RSHM", "RTDR", "RTRF", "RTTP",
    "S71A", "S85A", "S85R", "S86A", "S89G", "S89R", "S8OA", "SDRC", "SEAP", "SOUE",
    "SP1A", "SP6A", "SPEA", "SPRA", "SRRC", "SSFR", "SSRR", "STAL", "SU", "SUNA",
    "SUPC", "T70", "T90", "TAEA", "TBAB", "TCAL", "TCAS", "TCCA", "TEME", "TMBN",
    "TPAD", "TPDD", "TPDR", "TPDRI", "TPET", "TPMS", "TPOA", "TPSE", "TRDE", "TRFL",
    "TRMS", "TRNC", "TROA", "TRPP", "TRPT", "TRS", "TSEC", "TTBN", "TTBO", "TTCG",
    "TTCD", "TTJG", "TTPR", "TTPD", "UNPR", "UNDC", "UNDN", "UNDR", "UNTD", "WDLA",
    "WDLD", "WDLL", "WDRL", "WOAG", "WOAM", "WOAP", "WOAR", "WOPP", "WRDA", "X70",
    "X90", "XAAP", "XAEC", "XAWR", "XELG", "XELR", "XXCR", "XXXX", "XXSS", "ZZAX",
    "ZZBX", "ZZZX", "ZZZY", "ZZZZ"
])

def run_ph_code_search(cursor, catalog, schema, table, selected_codes, limit=10000, start_date=None, end_date=None):
    """
    Queries the prosecution history table for specific action codes.
    """
    if not selected_codes:
        return pd.DataFrame()

    # Format codes for SQL IN clause: 'CODE1', 'CODE2'
    codes_str = ", ".join([f"'{c}'" for c in selected_codes])
    
    query = f"""
    SELECT 
        serial_number,
        ph_action_code,
        ph_action_date,
        cm_desc,
        tm_worker_eid,
        ttab_tracking_num,
        ph_action_number,
        cm_sys_dt,
        last_modified_date
    FROM {catalog}.{schema}.{table}
    WHERE ph_action_code IN ({codes_str})
    and ph_action_date >= '{start_date}'
    and ph_action_date <= '{end_date}'
    ORDER BY serial_number, ph_action_date DESC
    LIMIT {limit}
    """
    
    try:
        cursor.execute(query)
        # Fetch logic using Arrow for speed if available
        try:
            return cursor.fetchall_arrow().to_pandas()
        except AttributeError:
            cols = [c[0] for c in cursor.description]
            return pd.DataFrame(cursor.fetchall(), columns=cols)
    except Exception as e:
        st.error(f"Query Error: {e}")
        return pd.DataFrame()

def generate_excel(df):
    """
    Converts DataFrame to Excel bytes for download.
    Disables URL conversion to prevent errors.
    FIX: Removes Timezones from datetime columns.
    """
    df = df.copy()
    
    # Identify datetime columns (including timezone-aware)
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            # Convert to timezone-naive (local time, dropping UTC offset)
            df[col] = df[col].dt.tz_localize(None)
            
    output = io.BytesIO()
    with pd.ExcelWriter(
        output, 
        engine='xlsxwriter',
        engine_kwargs={'options': {'strings_to_urls': False}}
    ) as writer:
        df.to_excel(writer, index=False, sheet_name='PH_Data')
        
        # Auto-adjust columns
        worksheet = writer.sheets['PH_Data']
        for i, col in enumerate(df.columns):
            worksheet.set_column(i, i, 20)
            
    return output.getvalue()