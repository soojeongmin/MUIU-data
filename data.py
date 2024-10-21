from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import pymysql
import time
import re

def init_driver():
    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--window-size=1920x1080')
    options.add_argument('--headless')

    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)

connection = pymysql.connect(
    host='aaaa',
    user='aaaa',
    password='aaaa',
    database='aaaa',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)

regions = [
    '서울특별시', '부산광역시', '대구광역시', '인천광역시', '광주광역시', '대전광역시', '울산광역시', '세종특별자치시',
    '경기도', '강원특별자치도', '충청북도', '충청남도', '전북특별자치도', '전라남도', '경상북도', '경상남도', '제주특별자치도'
]

for selected_region in regions:
    driver = init_driver()
    try:
        driver.get("https://www.safekorea.go.kr/idsiSFK/neo/sfk/cs/sfc/dis/disasterMsgList.jsp?menuSeq=679")

        try:
            wait = WebDriverWait(driver, 30)
            select_element = wait.until(EC.presence_of_element_located((By.ID, 'sbLawArea1')))
            select = Select(select_element)
            select.select_by_visible_text(selected_region)
            print(f"드롭다운 메뉴에서 '{selected_region}'를 성공적으로 선택했습니다.")
        except Exception as e:
            print(f"드롭다운 선택 과정에서 오류 발생: {e}")
            driver.quit()
            continue

        try:
            search_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id=\"content\"]/div[1]/a")))
            search_button.click()
            print("검색 버튼을 성공적으로 클릭했습니다.")
            time.sleep(5)
        except Exception as e:
            print(f"검색 버튼 클릭 과정에서 오류 발생: {e}")
            driver.quit()
            continue

        data = []

        while True:
            try:
                table = wait.until(EC.visibility_of_element_located((By.XPATH, "//*[@id='disasterSms_tr']")))
                rows = table.find_elements(By.XPATH, "//*[@id='disasterSms_tr']/tr[starts-with(@id, 'disasterSms_tr_')]")
                print("테이블 데이터를 성공적으로 가져왔습니다.")
            except Exception as e:
                print(f"테이블 데이터를 가져오는 과정에서 오류 발생: {e}")
                break

            for i, row in enumerate(rows):
                try:
                    columns = row.find_elements(By.TAG_NAME, "td")
                    alert_level = columns[1].text
                    event_content = columns[2].text
                    occurrence_time = columns[3].text
                    read_status = columns[4].text

                    message_content = driver.find_element(By.XPATH, f"//*[@id='disasterSms_tr_{i}_MSG_CN']").text

                    data.append({
                        "Category": selected_region,
                        "Alert Level": alert_level,
                        "Event Content": event_content,
                        "Occurrence Time": occurrence_time,
                        "Read Status": read_status,
                        "Message Content": message_content
                    })
                except Exception as e:
                    print(f"행 데이터 추출 과정에서 오류 발생: {e}")

            try:
                current_page_text = driver.find_element(By.ID, 'tbpageindex').text
                total_pages_text = driver.find_element(By.ID, 'tbpagetotal').text
                current_page = int(re.sub('[^0-9]', '', current_page_text))
                total_pages = int(re.sub('[^0-9]', '', total_pages_text))
                if current_page >= total_pages:
                    print("마지막 페이지입니다. 크롤링을 종료합니다.")
                    break
            except Exception as e:
                print(f"페이지 정보를 가져오는 과정에서 오류 발생: {e}")
                break

            try:
                next_button = driver.find_element(By.XPATH, "//*[@id='apagenext']")
                next_button.click()
                print("다음 페이지 버튼을 성공적으로 클릭했습니다.")
                time.sleep(3)
            except Exception as e:
                print("더 이상 페이지가 없습니다. 크롤링을 종료합니다.")
                break

        try:
            with connection.cursor() as cursor:
                insert_query = """
                    INSERT INTO disaster_messages (category, alert_level, event_content, occurrence_time, read_status, message_content)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                select_query = """
                    SELECT COUNT(*) as count FROM disaster_messages
                    WHERE category = %s AND alert_level = %s AND event_content = %s AND occurrence_time = %s AND read_status = %s AND message_content = %s
                """
                for item in data:
                    cursor.execute(select_query, (
                        item["Category"],
                        item["Alert Level"],
                        item["Event Content"],
                        item["Occurrence Time"],
                        item["Read Status"],
                        item["Message Content"]
                    ))
                    result = cursor.fetchone()
                    if result['count'] == 0:
                        cursor.execute(insert_query, (
                            item["Category"],
                            item["Alert Level"],
                            item["Event Content"],
                            item["Occurrence Time"],
                            item["Read Status"],
                            item["Message Content"]
                        ))
                connection.commit()
            print(f"{selected_region}의 크롤링 완료 및 데이터베이스에 저장되었습니다.")
        except Exception as e:
            print(f"데이터베이스에 데이터를 삽입하는 과정에서 오류 발생: {e}")

    except Exception as e:
        print(f"오류 발생: {e}")
    finally:
        driver.quit()

if connection.open:
    connection.close()
