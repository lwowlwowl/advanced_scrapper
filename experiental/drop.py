import pandas as pd

df_new = pd.read_csv('yahoo_links_1.csv')
df_prev = pd.read_csv('yahoo_articles_all.csv')

df_new_filtered = df_new[~df_new['url'].isin(df_prev['url'])]
df_new_filtered = df_new_filtered.drop_duplicates(subset='url')
df_new_filtered['date_time'] = df_new_filtered['date_time'].astype(int)
df_new_filtered = df_new_filtered.sort_values(by='date_time', ascending=False)
df_new_filtered.reset_index(drop=True, inplace=True)
df_new_filtered.to_csv('yahoo_links_new.csv', index=False)
