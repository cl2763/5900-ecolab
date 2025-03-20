import pandas as pd

def merge_classification_and_citation(
    classification_file='./data//filtered/inspections_classifications_filtered.csv',
    citation_file='./data//filtered/inspections_citations_filtered.csv',
    output_file="merged_dataset.csv"
):
    """
    将 Citation 信息合并到 Classification，基于 'FEINumber' 做 LEFT JOIN。
    若 Citation 中同一个 FEINumber 存在多条记录，则会“复制”相应的 Classification 行。
    删除 Classification 中的 'ProductType' 列，删除 Citation 中的 'ProgramArea' 列。
    如果需要合并同名列，请根据需要配置 same_columns 列表。
    """

    # ========== 1. 读取两张表 ==========
    df_class = pd.read_csv(classification_file)
    df_cit = pd.read_csv(citation_file)

    # ========== 2. 删除不需要的列 ==========
    # 从 Classification 删除 'ProductType'
    cols_to_drop_class = []
    if 'Address 2' in df_class.columns:
        cols_to_drop_class.append('Address 2')
    if 'Classification' in df_class.columns:
        cols_to_drop_class.append('Classification')
    if 'ProductType' in df_class.columns:
        cols_to_drop_class.append('ProductType')
    df_class.drop(columns=cols_to_drop_class, inplace=True, errors='ignore')
    
    # 同理，在 Citation 中删除 'Address line 2' 等
    if 'Address line 2' in df_cit.columns:
        df_cit.drop(columns=['Address line 2'], inplace=True, errors='ignore')
    if 'ProgramArea' in df_cit.columns:
        df_cit.drop(columns=['ProgramArea'], inplace=True, errors='ignore')

    # ========== 3. 合并（JOIN）==========
    # 以 Classification 为主表（left join），基于 FEINumber
    # Citation 中若有多个匹配行，会“复制”该条 Classification 行，多对多展开
    df_merged = pd.merge(
        df_class,
        df_cit,
        on="FEINumber",  # 请确认列名是否完全一致
        how="left",
        suffixes=("_class", "_cit")  # 避免同名列冲突
    )

    # ========== 4. 如需合并同名列（可选）==========
    # 若两表有相同列名，如 'City', 'State' 等，可以将其合并成单列。
    # 示例中，假设这几个列都在两表出现，我们优先使用 classification 中的值，否则用 citation 中的值。
    same_columns = [
        "City",
        "State",
        "ZipCode",
        "LegalName",
        "AddressLine1",
        "AddressLine2",
        "StateCode",
        "CountryCode",
        "CountryName",
        "InspectionID",
        "InspectionEndDate",
        "FiscalYear",
        "FirmProfile"
        # 如有更多同名列可加入这里
    ]
    for col in same_columns:
        col_class = col + "_class"
        col_cit = col + "_cit"
        if col_class in df_merged.columns and col_cit in df_merged.columns:
            # combine_first：class 不为空就用它，否则用 citation 的值
            df_merged[col] = df_merged[col_class].combine_first(df_merged[col_cit])
            # 合并后删除原先的两个列
            df_merged.drop(columns=[col_class, col_cit], inplace=True)

    # ========== 5. 保存结果 ==========
    df_merged.to_csv(output_file, index=False)
    print(f"合并完成，结果已保存到 {output_file}")


if __name__ == "__main__":
    merge_classification_and_citation()
