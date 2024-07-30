import pandas as pd
import numpy as np
import yaml
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import MinMaxScaler, StandardScaler
from scipy.stats import boxcox

#Params.
def load_config(config_path: str):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)
config = load_config('config.yaml')

db_path = config['database']['result_db_for_API']



def get_full_data(db_path, table_name):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [col[1] for col in cursor.fetchall()]
    
    query = f"SELECT * FROM {table_name}"
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    data = [dict(zip(columns, row)) for row in results]
    return data

def finding_latest_entries(full_df):
    latest_tasks = full_df.groupby('qid').apply(lambda x: x.loc[x.index.max()])
    task_list = latest_tasks['task_id'].tolist()
    latest_entries = full_df[full_df['task_id'].isin(task_list)]
    return latest_entries

full_df = pd.DataFrame(get_full_data(db_path, 'aggregated_results')).set_index('id')
latest_entries = finding_latest_entries(full_df)
values_counts = latest_entries.groupby('qid')['result'].value_counts().unstack(fill_value=0)

df = values_counts.copy()
def plot_distributions(df, title):
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    fig.suptitle(title)
    for i, col in enumerate(df.columns):
        axes[i].hist(df[col], bins=20, density=True)
        axes[i].set_title(col)
    plt.tight_layout()
    plt.show()

plot_distributions(df, "Original Distributions")

# 1. Min-Max Scaling
scaler = MinMaxScaler()
df_minmax = pd.DataFrame(scaler.fit_transform(df), columns=df.columns)
plot_distributions(df_minmax, "Min-Max Scaled Distributions")

# 2. Z-Score Normalization
scaler = StandardScaler()
df_zscore = pd.DataFrame(scaler.fit_transform(df), columns=df.columns)
plot_distributions(df_zscore, "Z-Score Normalized Distributions")

# 3. Log Transformation
df_log = df.apply(lambda x: np.log1p(x))
plot_distributions(df_log, "Log Transformed Distributions")

# 4. Box-Cox Transformation
df_boxcox = df.copy()
for col in df_boxcox.columns:
    df_boxcox[col], _ = boxcox(df_boxcox[col] + 1)  # 값이 0 이상이어야 함
plot_distributions(df_boxcox, "Box-Cox Transformed Distributions")

# Boxplot for all transformations
plt.figure(figsize=(15, 10))
df_melted = pd.melt(df, var_name='Result', value_name='Original')
df_melted['Min-Max'] = pd.melt(df_minmax)['value']
df_melted['Z-Score'] = pd.melt(df_zscore)['value']
df_melted['Log'] = pd.melt(df_log)['value']
df_melted['Box-Cox'] = pd.melt(df_boxcox)['value']

df_melted_long = pd.melt(df_melted, id_vars=['Result'], var_name='Transformation', value_name='Value')

sns.boxplot(x='Result', y='Value', hue='Transformation', data=df_melted_long)
plt.title('Distribution of Values for Each Result and Transformation')
plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.show()