import streamlit as st
import pandas as pd
import requests
import snowflake.connector
from typing import Dict, Any, List, Optional
import os
import time

MAX_ATTEMPTS_MAIN = 100
MAX_HINTS = 2

# Cortex Analyst設定
DATABASE = "SNOWFLAKE_LEARNING_DB"
SCHEMA = "CORTEX_ANALYST_DEMO"
STAGE = "RAW_DATA"
FILE = "semantic_model_J_CI_FD20.yaml"

# === ユーティリティ関数（utilsモジュールの代替） ===

def header_animation():
    """ヘッダーアニメーション"""
    placeholder = st.empty()
    for i in range(3):
        placeholder.markdown(f"{'🔥' * (i+1)}")
        time.sleep(0.1)
    placeholder.empty()

def display_problem_statement_swt25(text: str):
    """問題文を表示"""
    st.markdown(
        f"""
        <div style="background-color: #787c80; padding: 20px; border-radius: 10px; border-left: 5px solid #4169e1;">
            <i>"序列に隠された真実への道標。<br/>
その位置を正確に見抜くことで、データの扉は開かれる。"</i><br/><br/>

2020年国勢調査が記した47都道府県の人口序列。<br/>
上位でも下位でもない、ちょうど20番目という絶妙な位置に存在する地域。<br/>
その名を突き止めよ。<br/><br/>

<b>ヒント：Cortex Analystの刀に2回まで質問可能。データを賢く分析し、答えを導き出せ。</b>
        </div>
        """,
        unsafe_allow_html=True
    )

def init_state(tab_name: str) -> Dict:
    """状態を初期化"""
    if f'{tab_name}_state' not in st.session_state:
        st.session_state[f'{tab_name}_state'] = {
            'tab_name': tab_name,
            'is_clear': False,
            'attempts': 0
        }
    return st.session_state[f'{tab_name}_state']

def save_state(state: Dict):
    """状態を保存"""
    tab_name = state.get('tab_name', 'q6_test')
    st.session_state[f'{tab_name}_state'] = state

# === Snowflake接続関数 ===

def get_snowflake_connection():
    """Snowflake接続を取得（ローカル/Streamlit Cloud両対応）"""
    try:
        # Streamlit Secretsから接続
        if hasattr(st, 'secrets') and 'snowflake' in st.secrets:
            return snowflake.connector.connect(
                user=st.secrets.snowflake.user,
                password=st.secrets.snowflake.password,
                account=st.secrets.snowflake.account,
                host=st.secrets.snowflake.get('host', 'FSUOFLI-SQ50969.snowflakecomputing.com'),
                port=st.secrets.snowflake.get('port', 443),
                warehouse=st.secrets.snowflake.get('warehouse', 'COMPUTE_WH'),
                role=st.secrets.snowflake.get('role', 'ACCOUNTADMIN'),
                database=DATABASE,
                schema=SCHEMA
            )
        # 環境変数から接続
        else:
            return snowflake.connector.connect(
                user=os.getenv('SNOWFLAKE_USER'),
                password=os.getenv('SNOWFLAKE_PASSWORD'),
                account=os.getenv('SNOWFLAKE_ACCOUNT'),
                host=os.getenv('SNOWFLAKE_HOST', 'FSUOFLI-SQ50969.snowflakecomputing.com'),
                port=int(os.getenv('SNOWFLAKE_PORT', '443')),
                warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
                role=os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN'),
                database=DATABASE,
                schema=SCHEMA
            )
    except Exception as e:
        st.error(f"Snowflake接続エラー: {str(e)}")
        return None

# === Cortex Analyst関連関数 ===

def send_cortex_message(prompt: str, connector) -> Optional[Dict[str, Any]]:
    """Cortex Analystにメッセージを送信"""
    try:
        request_body = {
            "messages": [{"role": "user", "content": [{"type": "text", "text": prompt}]}],
            "semantic_model_file": f"@{DATABASE}.{SCHEMA}.{STAGE}/{FILE}",
        }
        
        # ホスト情報を取得
        host = getattr(connector, 'host', 'FSUOFLI-SQ50969.snowflakecomputing.com')
        
        # トークンを取得
        if hasattr(connector, 'rest') and hasattr(connector.rest, 'token'):
            token = connector.rest.token
        else:
            # トークンが取得できない場合はエラー
            st.error("認証トークンの取得に失敗しました")
            return None
        
        resp = requests.post(
            url=f"https://{host}/api/v2/cortex/analyst/message",
            json=request_body,
            headers={
                "Authorization": f'Snowflake Token="{token}"',
                "Content-Type": "application/json",
            },
            timeout=30
        )
        
        if resp.status_code < 400:
            return resp.json()
        else:
            st.error(f"Cortex Analyst API Error: {resp.status_code}")
            return None
            
    except Exception as e:
        st.error(f"Cortex Analystエラー: {str(e)}")
        return None

def display_cortex_content(content: List[Dict[str, str]], connector) -> None:
    """Cortexレスポンスを表示"""
    for item in content:
        if item["type"] == "text":
            st.markdown(item["text"])
        elif item["type"] == "sql":
            with st.expander("SQLクエリ", expanded=False):
                st.code(item["statement"], language="sql")
            with st.expander("実行結果", expanded=True):
                try:
                    with st.spinner("SQL実行中..."):
                        df = pd.read_sql(item["statement"], connector)
                        st.dataframe(df, use_container_width=True)
                except Exception as e:
                    st.error(f"SQL実行エラー: {str(e)}")

# === メイン関数 ===

def present_quiz(tab_name: str = "q6_test") -> str:
    """クイズ問題を表示"""
    
    # Snowflake接続を取得
    if 'snowflake_conn' not in st.session_state or st.session_state.snowflake_conn is None:
        st.session_state.snowflake_conn = get_snowflake_connection()
    
    connector = st.session_state.snowflake_conn
    
    header_animation()
    st.header(":blue[人口統計の鬼] 〜データ分析の呼吸〜", divider="blue")
    
    display_problem_statement_swt25(
    """
    <i>"序列に隠された真実への道標。<br/>
その位置を正確に見抜くことで、データの扉は開かれる。"</i><br/><br/>

2020年国勢調査が記した47都道府県の人口序列。<br/>
上位でも下位でもない、ちょうど20番目という絶妙な位置に存在する地域。<br/>
その名を突き止めよ。<br/><br/>

<b>ヒント：Cortex Analystの刀に2回まで質問可能。データを賢く分析し、答えを導き出せ。</b>
    """
    )
    
    # データサンプル表示
    if connector:
        with st.expander("📊 データテーブル構造を確認", expanded=False):
            try:
                sample_query = f"""
                SELECT * FROM {DATABASE}.{SCHEMA}.J_CI_FD20 
                LIMIT 5
                """
                sample_df = pd.read_sql(sample_query, connector)
                st.dataframe(sample_df, use_container_width=True)
                st.caption(f"データソース: {DATABASE}.{SCHEMA}.J_CI_FD20")
            except Exception as e:
                st.error(f"データ取得エラー: {str(e)}")
    else:
        st.warning("データベースに接続されていません")
    
    # セッション状態の初期化
    if f'{tab_name}_hint_count' not in st.session_state:
        st.session_state[f'{tab_name}_hint_count'] = 0
    if f'{tab_name}_hints_history' not in st.session_state:
        st.session_state[f'{tab_name}_hints_history'] = []
    
    # ヒント状態の表示
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("ヒント使用回数", f"{st.session_state[f'{tab_name}_hint_count']} / {MAX_HINTS}")
    with col2:
        remaining = MAX_HINTS - st.session_state[f'{tab_name}_hint_count']
        st.metric("残りヒント", remaining)
    with col3:
        if remaining == 0:
            st.warning("ヒント枯渇")
        else:
            st.info("ヒント利用可能")
    
    # ヒント機能
    st.markdown("### 💡 Cortex Analyst の刀システム")
    
    if st.session_state[f'{tab_name}_hint_count'] < MAX_HINTS:
        hint_question = st.text_input(
            "Cortex Analystの刀への質問",
            placeholder="例: 2020年の人口ランキング15位から25位を表示して",
            key=f"{tab_name}_hint_input"
        )
        
        col1, col2 = st.columns([1, 5])
        with col1:
            get_hint = st.button(
                "ヒント取得",
                key=f"{tab_name}_get_hint",
                type="primary",
                disabled=(st.session_state[f'{tab_name}_hint_count'] >= MAX_HINTS or not connector)
            )
        
        if get_hint and hint_question and connector:
            st.session_state[f'{tab_name}_hint_count'] += 1
            with st.spinner("Cortex Analystが分析中..."):
                response = send_cortex_message(hint_question, connector)
                if response:
                    hint_result = {
                        'question': hint_question,
                        'response': response["message"]["content"]
                    }
                    st.session_state[f'{tab_name}_hints_history'].append(hint_result)
                    
                    st.success(f"ヒント {st.session_state[f'{tab_name}_hint_count']} 取得完了")
                    display_cortex_content(response["message"]["content"], connector)
                else:
                    st.session_state[f'{tab_name}_hint_count'] -= 1
    else:
        st.warning("⚠️ ヒントの使用回数が上限に達しました。自力で解答してください。")
    
    # 過去のヒント表示
    if st.session_state[f'{tab_name}_hints_history']:
        with st.expander("📜 取得済みヒント履歴", expanded=False):
            for i, hint in enumerate(st.session_state[f'{tab_name}_hints_history'], 1):
                st.markdown(f"**ヒント {i}: {hint['question']}**")
                if connector:
                    display_cortex_content(hint['response'], connector)
                st.markdown("---")
    
    # 回答入力
    st.markdown("---")
    st.markdown("### 🎯 最終回答")
    answer = st.text_input(
        "都道府県名を入力",
        placeholder="例: 東京都、大阪府、青森県",
        key=f"{tab_name}_answer_input"
    )
    
    return answer

def process_answer(answer: str, state: Dict) -> None:
    """回答を処理"""
    correct_answer = "岡山県"
    
    if answer and answer.strip():
        state['attempts'] = state.get('attempts', 0) + 1
        
        if answer.strip() == correct_answer:
            state["is_clear"] = True
            st.balloons()
            st.success(f"**討伐成功！** 正解は{correct_answer}でした！データ分析の鬼を撃破した！")
        else:
            state["is_clear"] = False
            st.error(f"**討伐失敗！** 「{answer}」は不正解... 鬼に惑わされた。")
            
            # ヒントが残っている場合は案内
            tab_name = state.get("tab_name", "q6_test")
            remaining = MAX_HINTS - st.session_state.get(f'{tab_name}_hint_count', 0)
            if remaining > 0:
                st.info(f"💡 ヒントをあと{remaining}回使用できます。")
    else:
        st.warning("回答を入力してください")
    
    save_state(state)

def run(tab_name: str = "q6_test"):
    """メイン実行関数（スタンドアロン版）"""
    state = init_state(tab_name)
    
    answer = present_quiz(tab_name)
    
    # 試行回数のチェック
    if state.get('attempts', 0) >= MAX_ATTEMPTS_MAIN:
        st.error(f"試行回数が上限（{MAX_ATTEMPTS_MAIN}回）に達しました。")
        if st.button("リセット", key=f"{tab_name}_reset"):
            state['attempts'] = 0
            state['is_clear'] = False
            save_state(state)
            st.rerun()
    else:
        if st.button("討伐開始", key=f"{tab_name}_submit", type="primary"):
            process_answer(answer, state)
            st.rerun()
        
        # 状態表示
        if state.get('is_clear', False):
            st.success("クリア済み！")

# === エントリーポイント ===

if __name__ == "__main__":
    st.set_page_config(
        page_title="人口統計の鬼",
        page_icon="🗾",
        layout="wide"
    )
    
    # .envファイルから環境変数を読み込む（オプション）
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    
    run()
