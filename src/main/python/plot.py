import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv('traffic_data.csv')

# Primo grafico: Average Bytes per Traffic Class
plt.figure(figsize=(10,6))
for label in df['label'].unique():
    subset = df[df['label'] == label]
    plt.bar(subset['traffic_class'], subset['avg_bytes'], alpha=0.7, label=label)
plt.yscale('log')
plt.title('Average Bytes per Traffic Class (log scale)')
plt.xlabel('Traffic Class')
plt.ylabel('Average Bytes (log scale)')
plt.xticks(rotation=45)
plt.legend()
plt.tight_layout()
# plt.show()
plt.savefig('avg_bytes_per_traffic_class_log_scale.png')

# Secondo grafico: Number of Connections per Traffic Class (con scala logaritmica)
plt.figure(figsize=(10,6))
for label in df['label'].unique():
    subset = df[df['label'] == label]
    plt.bar(subset['traffic_class'], subset['count'], alpha=0.7, label=label)
plt.yscale('log')
plt.title('Number of Connections per Traffic Class (log scale)')
plt.xlabel('Traffic Class')
plt.ylabel('Number of Connections (log scale)')
plt.xticks(rotation=45)
plt.legend()
plt.tight_layout()
# plt.show()
plt.savefig('connections_per_traffic_class_log_scale.png')
