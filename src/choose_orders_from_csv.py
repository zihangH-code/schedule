from pathlib import Path
import pandas as pd

# ===== 可直接修改的参数 =====
NUM = 25
INPUT_CSV = Path("D:/A_sch/data/orders.csv")
OUTPUT_CSV = Path(f"D:/A_sch/data/orders_{NUM}.csv")


def main() -> None:
    if not INPUT_CSV.exists():
        raise FileNotFoundError(f"未找到输入文件: {INPUT_CSV}")

    df = pd.read_csv(INPUT_CSV)

    if NUM <= 0:
        raise ValueError("NUM 必须大于 0")

    result_df = df.head(NUM).copy()

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    result_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

    print(f"输入文件: {INPUT_CSV}")
    print(f"原始行数: {len(df)}")
    print(f"输出行数: {len(result_df)}")
    print(f"输出文件: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()

