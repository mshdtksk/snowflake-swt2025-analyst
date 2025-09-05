from typing import Any, Dict, List, Optional
import os
import pandas as pd
import requests
import snowflake.connector
import streamlit as st

# 設定値（認証情報以外）
HOST = os.getenv("SNOWFLAKE_HOST", "FSUOFLI-SQ50969.snowflakecomputing.com")
DATABASE = "SNOWFLAKE_LEARNING_DB"
SCHEMA = "CORTEX_ANALYST_DEMO"
STAGE = "RAW_DATA"
FILE = "semantic_model_J_CI_FD20.yaml"

def get_session():
    """
    認証情報を環境変数またはStreamlit Secretsから取得してセッションを作成
    """
    # Streamlit Secretsから取得を試みる（Streamlit Cloudの場合）
    if hasattr(st, 'secrets') and 'snowflake' in st.secrets:
        return snowflake.connector.connect(
            user=st.secrets.snowflake.user,
            password=st.secrets.snowflake.password,
            account=st.secrets.snowflake.account,
            host=st.secrets.snowflake.get('host', HOST),
            port=st.secrets.snowflake.get('port', 443),
            warehouse=st.secrets.snowflake.get('warehouse', 'COMPUTE_WH'),
            role=st.secrets.snowflake.get('role', 'ACCOUNTADMIN'),
        )
    # 環境変数から取得（GitHub Actions/ローカル開発の場合）
    else:
        return snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            host=os.getenv('SNOWFLAKE_HOST', HOST),
            port=int(os.getenv('SNOWFLAKE_PORT', '443')),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
            role=os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN'),
        )

# データベース接続
if 'CONN' not in st.session_state or st.session_state.CONN is None:
    try:
        st.session_state.CONN = get_session()
    except Exception as e:
        st.error(f"データベース接続エラー: {str(e)}")
        st.stop()

# セッション状態の初期化
if 'hint_count' not in st.session_state:
    st.session_state.hint_count = 0
if 'game_over' not in st.session_state:
    st.session_state.game_over = False
if 'messages' not in st.session_state:
    st.session_state.messages = []

def send_message(prompt: str) -> Dict[str, Any]:
    """Calls the REST API and returns the response."""
    request_body = {
        "messages": [{"role": "user", "content": [{"type": "text", "text": prompt}]}],
        "semantic_model_file": f"@{DATABASE}.{SCHEMA}.{STAGE}/{FILE}",
    }
    
    # ホスト名を環境変数から取得
    api_host = os.getenv('SNOWFLAKE_HOST', HOST)
    
    resp = requests.post(
        url=f"https://{api_host}/api/v2/cortex/analyst/message",
        json=request_body,
        headers={
            "Authorization": f'Snowflake Token="{st.session_state.CONN.rest.token}"',
            "Content-Type": "application/json",
        },
    )
    request_id = resp.headers.get("X-Snowflake-Request-Id")
    if resp.status_code < 400:
        return {**resp.json(), "request_id": request_id}
    else:
        raise Exception(
            f"Failed request (id: {request_id}) with status {resp.status_code}: {resp.text}"
        )

def display_content(content: List[Dict[str, str]]) -> None:
    """Displays a content item for a message."""
    for item in content:
        if item["type"] == "text":
            st.markdown(item["text"])
        elif item["type"] == "sql":
            with st.expander("SQL Query", expanded=False):
                st.code(item["statement"], language="sql")
            with st.expander("Results", expanded=True):
                with st.spinner("Running SQL..."):
                    df = pd.read_sql(item["statement"], st.session_state.CONN)
                    st.dataframe(df)

# タイトルとクイズの説明
st.title("都道府県人口クイズ")
st.markdown("### 問題：2020年の都道府県の人口で20位の都道府県は？")

# データサンプルの表示
st.info("データサンプル（PREPPER_OPEN_DATA_BANK__JAPANESE_CITY_DATA.E_PODB.E_CI_FD20）")
try:
    sample_query = """
    SELECT * FROM SNOWFLAKE_LEARNING_DB.CORTEX_ANALYST_DEMO.J_CI_FD20 
    LIMIT 5
    """
    sample_df = pd.read_sql(sample_query, st.session_state.CONN)
    st.dataframe(sample_df)
except Exception as e:
    st.warning("サンプルデータの取得に失敗しました")

# ゲーム状態の表示
col1, col2 = st.columns(2)
with col1:
    st.metric("ヒント使用回数", f"{st.session_state.hint_count}/3")
with col2:
    if st.session_state.game_over:
        st.success("ゲーム終了！")

# ヒント機能
st.markdown("---")
st.markdown("### ヒント機能（Cortex Analyst）")
st.markdown(f"残りヒント回数: **{3 - st.session_state.hint_count}回**")

if st.session_state.hint_count < 3 and not st.session_state.game_over:
    hint_question = st.text_input(
        "Cortex Analystに質問してヒントを得る（例：「2020年の人口ランキング15位から25位を表示して」）",
        key="hint_input"
    )
    if st.button("ヒントを取得", disabled=(st.session_state.hint_count >= 3)):
        if hint_question:
            st.session_state.hint_count += 1
            with st.spinner("Cortex Analystに問い合わせ中..."):
                try:
                    response = send_message(hint_question)
                    st.markdown(f"**ヒント {st.session_state.hint_count}:**")
                    display_content(response["message"]["content"])
                except Exception as e:
                    st.error(f"エラーが発生しました: {str(e)}")
        else:
            st.warning("質問を入力してください")
else:
    if st.session_state.hint_count >= 3:
        st.warning("ヒントの使用回数が上限に達しました")

# 回答欄
st.markdown("---")
st.markdown("### 回答")

answer = st.text_input("都道府県名を入力してください（例：東京都、大阪府、青森県など）", key="answer_input")

if st.button("回答する", disabled=st.session_state.game_over):
    if answer:
        if answer == "岡山県":
            st.balloons()
            st.success("正解です！2020年の人口20位は岡山県でした！")
            st.session_state.game_over = True
        else:
            st.error(f"残念！「{answer}」は不正解です。もう一度考えてみましょう。")
            if st.session_state.hint_count < 3:
                st.info(f"ヒントをあと{3 - st.session_state.hint_count}回使用できます。")
    else:
        st.warning("回答を入力してください")