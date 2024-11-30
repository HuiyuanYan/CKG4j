from wordcloud import WordCloud
import matplotlib.pyplot as plt

# 配置输入和输出文件名
input_file = "task4/pagerank_results"  # 原始数据文件名
output_image = "wordcloudtt.png"  # 生成的词云图片名

#windows系统自带字体路径，可以自行更改
font_path = "C:/Windows/Fonts/msyh.ttc"  # 微软雅黑字体路径

# 读取并预处理数据
def preprocess_data(file_path):
    word_freq = {}
    with open(file_path, "r", encoding="utf-8") as file:
        lines = file.readlines()
    for line in lines:
        if "|" in line:
            name, weight = line.split("|")
            weight = float(weight.strip())  # 转换权重为浮点数
            word_freq[name] = weight
    return word_freq

# 生成词云
def create_wordcloud(word_freq, output_path,font_path):
    wordcloud = WordCloud(
        width=800, height=400,
        background_color="white",
        font_path=font_path,
        colormap="viridis"
    ).generate_from_frequencies(word_freq)  # 使用频率生成词云

    # 显示词云
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis("off")
    plt.show()

    # 保存词云图片
    wordcloud.to_file(output_path)
    print(f"词云图片已保存为 {output_path}")

# 主流程
if __name__ == "__main__":
    # 读取并处理数据
    word_freq = preprocess_data(input_file)
    # 生成词云并保存
    create_wordcloud(word_freq, output_image,font_path)
