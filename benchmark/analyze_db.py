import matplotlib.pyplot as plt
import numpy as np

# Исходные данные в КБ
data_kb = {
    "1GB": {
        "Random":  {"Yadro": 1065064,  "SQLite": 1079768},
        "Stealth": {"Yadro": 317471,   "SQLite": 1169616}
    },
    "10GB": {
        "Random":  {"Yadro": 10668080, "SQLite": 10799226},
        "Stealth": {"Yadro": 3164469,  "SQLite": 11698960}
    }
}

def kb_to_mb(kb):
    return kb / 1024

# Расчет коэффициентов сжатия
print("=== АНАЛИЗ КОЭФФИЦИЕНТОВ СЖАТИЯ (SQLite / YADRO) ===")
for scale in ["1GB", "10GB"]:
    print(f"\n--- Масштаб данных: {scale} ---")
    for test_type in ["Random", "Stealth"]:
        yadro_kb = data_kb[scale][test_type]["Yadro"]
        sqlite_kb = data_kb[scale][test_type]["SQLite"]
        
        # Считаем, во сколько раз YADRO меньше SQLite
        compression_ratio = sqlite_kb / yadro_kb
        print(f"Тест [{test_type}]:")
        print(f"  YADRO DB: {kb_to_mb(yadro_kb):.2f} MB")
        print(f"  SQLite:   {kb_to_mb(sqlite_kb):.2f} MB")
        print(f"  Коэффициент преимущества (сжатия): {compression_ratio:.2f}x")

# --- ПОСТРОЕНИЕ ГРАФИКОВ ---
# Настройка стиля (по желанию можно использовать 'ggplot', 'seaborn-whitegrid' и т.д.)
plt.style.use('bmh')

fig, axes = plt.subplots(1, 2, figsize=(14, 6))
fig.suptitle('Сравнение объема базы данных на диске (Меньше — лучше)', fontsize=16, fontweight='bold')

labels = ['Тест Random\n(Высокая энтропия)', 'Тест Stealth\n(Скрытые паттерны)']
x = np.arange(len(labels))
width = 0.35  # Ширина столбцов

colors_yadro = '#2ecc71' # Зеленый для своей БД
colors_sqlite = '#e74c3c' # Красный для SQLite

for i, scale in enumerate(["1GB", "10GB"]):
    ax = axes[i]
    
    # Извлекаем и переводим в МБ
    yadro_mb = [kb_to_mb(data_kb[scale]["Random"]["Yadro"]), kb_to_mb(data_kb[scale]["Stealth"]["Yadro"])]
    sqlite_mb = [kb_to_mb(data_kb[scale]["Random"]["SQLite"]), kb_to_mb(data_kb[scale]["Stealth"]["SQLite"])]
    
    # Отрисовка столбцов
    rects1 = ax.bar(x - width/2, yadro_mb, width, label='YADRO DB (LZ4)', color=colors_yadro, edgecolor='black')
    rects2 = ax.bar(x + width/2, sqlite_mb, width, label='SQLite', color=colors_sqlite, edgecolor='black')
    
    ax.set_ylabel('Размер на диске (Мегабайты)')
    ax.set_title(f'Объем входных данных: ~{scale}')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()
    
    # Добавление значений над столбцами
    def autolabel(rects):
        for rect in rects:
            height = rect.get_height()
            ax.annotate(f'{height:.0f} MB',
                        xy=(rect.get_x() + rect.get_width() / 2, height),
                        xytext=(0, 3),  # 3 пункта смещение по вертикали
                        textcoords="offset points",
                        ha='center', va='bottom', fontsize=10, fontweight='bold')
            
    autolabel(rects1)
    autolabel(rects2)

plt.tight_layout()
# Сохраняем график в файл (удобно для вставки в отчет)
plt.savefig('db_benchmark_results.png', dpi=300, bbox_inches='tight')
plt.show()