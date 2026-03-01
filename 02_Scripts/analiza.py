import pandas as pd
import os

input_file = '01_Raw_Data/Financial-data-supplement-Q3_2025.xlsx'
output_file = '03_Processed_Data/procredit_final_analysis.csv'

def run_analysis():
    if not os.path.exists(input_file):
        print("Фајлот не е пронајден!")
        return

    tasks = [
        {"index": 9, "label": "SEE Region"},
        {"index": 14, "label": "North Macedonia"},
        {"index": 17, "label": "EE Region"},
    ]
    
    # Дефинираме специфични клучни зборови за да нема мешање
    metrics_search = [
        # Проценти
        {'key': 'Return on average equity', 'short': 'ROE_pct'},
        {'key': 'Credit-impaired loans', 'short': 'NPL_Ratio_pct'},
        {'key': 'Cost-income ratio', 'short': 'CIR_pct'},
        {'key': 'Net interest margin', 'short': 'NIM_pct'},
        # Милиони евра
        {'key': 'Total assets', 'short': 'Total_Assets_Mln'},
        {'key': 'Customer loan portfolio', 'short': 'Loan_Portfolio_Mln'},
        {'key': 'Customer deposits', 'short': 'Deposits_Mln'},
        {'key': 'Total equity', 'short': 'Total_Equity_Mln'}
    ]

    all_data = []

    for task in tasks:
        print(f"🔍 Обработувам {task['label']} (Таб {task['index']})...")
        try:
            # Читаме сè како стринг за да избегнеме float грешки при пребарување
            df = pd.read_excel(input_file, sheet_name=task['index'], header=None, dtype=str)
            
    
            header_row_idx = None
            date_mapping = {}
            for r_idx in range(15):
                row = df.iloc[r_idx].tolist()
                cols = [i for i, val in enumerate(row) if val and ('20' in str(val) or 'Q' in str(val))]
                if len(cols) >= 2:
                    header_row_idx = r_idx
                    date_mapping = {i: str(row[i]).strip() for i in cols}
                    break

            if header_row_idx is None: continue

            # Скенирање низ редовите за нашите метрики
            for r_idx in range(header_row_idx + 1, len(df)):
                # Проверуваме во првите две колони
                indicator_name = str(df.iloc[r_idx, 0]).strip()
                if indicator_name == 'nan' or not indicator_name:
                    indicator_name = str(df.iloc[r_idx, 1]).strip()

                for m in metrics_search:
                    # Користиме точно совпаѓање на почетокот на името за да не мешаме Total Equity со Return on Equity
                    if m['key'].lower() in indicator_name.lower():
                        # Дополнителна заштита: ако бараме 'Total Equity', не сакаме да го фатиме 'Return on Equity'
                        if m['short'] == 'Total_Equity_Mln' and 'return' in indicator_name.lower():
                            continue
                            
                        for c_idx, date_label in date_mapping.items():
                            val = df.iloc[r_idx, c_idx]
                            try:
                                if pd.isna(val) or str(val).strip() in ['-', '', 'nan']: continue
                                
                                # Чистење на бројката
                                clean_val = str(val).replace('%', '').replace(',', '').strip()
                                
                                all_data.append({
                                    'Date': date_label,
                                    'Metric': m['short'],
                                    'Value': float(clean_val),
                                    'Source': task['label'],
                                    'Unit': 'Percentage' if '_pct' in m['short'] else 'EUR_Millions'
                                })
                            except:
                                continue
        except Exception as e:
            print(f"❌ Грешка кај {task['label']}: {e}")

    if all_data:
        final_df = pd.DataFrame(all_data)
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        final_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n🚀 ГОТОВО! Снимени {len(final_df)} записи со милиони и проценти.")
    else:
        print("\n❌ Нема податоци.")

if __name__ == "__main__":
    run_analysis()