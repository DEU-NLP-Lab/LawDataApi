from llama_parse import LlamaParse
from llama_index.core import SimpleDirectoryReader

import os
import nest_asyncio
from dotenv import load_dotenv


def save_documents_as_markdown(documents, output_folder):
    """
    PDF 문서 리스트를 개별 Markdown 파일로 저장하는 함수

    Args:
        documents: PDF 문서 객체 리스트
        output_folder: Markdown 파일을 저장할 폴더 경로
    """
    import os

    # 출력 폴더가 없으면 생성
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # 각 문서를 순회하며 Markdown 파일로 저장
    for i, doc in enumerate(documents):
        # 파일명 생성 (문서 인덱스 기반)
        filename = f"document_{i+1}.md"
        filepath = os.path.join(output_folder, filename)

        # 문서 내용을 Markdown 파일로 저장
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(doc.text)

        print(f"문서 {i+1}을(를) {filepath}에 저장했습니다.")


def pdf_file_load(folder_path):
    """
    폴더 내의 모든 PDF 파일을 로드하고 파싱합니다.

    Args:
        folder_path (str): PDF 파일이 있는 폴더 경로

    Returns:
        dict: PDF 파일명을 키로, 해당 파일의 파싱된 문서 리스트를 값으로 하는 딕셔너리
    """
    try:
        # API 키 로드
        api_key_load()

        # 폴더 존재 여부 확인
        if not os.path.exists(folder_path):
            print(f"'{folder_path}' 폴더가 존재하지 않습니다.")
            return {}

        if not os.path.isdir(folder_path):
            print(f"'{folder_path}'는 폴더가 아닙니다.")
            return {}

        # 파서 설정
        parser = LlamaParse(
            result_type="markdown",  # "markdown"과 "text" 사용 가능
            num_workers=8,  # worker 수 (기본값: 4)
            verbose=True,
            language="ko",
        )

        # PDF 파일 추출기 설정
        file_extractor = {".pdf": parser}

        # 폴더 내의 모든 PDF 파일 경로 가져오기
        pdf_files = []
        for file in os.listdir(folder_path):
            if file.lower().endswith(".pdf"):
                pdf_files.append(os.path.join(folder_path, file))

        if not pdf_files:
            print(f"'{folder_path}' 폴더에 PDF 파일이 없습니다.")
            return {}

        print(
            f"'{folder_path}' 폴더에 {len(pdf_files)} 개의 PDF 파일이 로드되었습니다."
        )

        # 각 PDF 파일별로 개별 처리
        result_dict = {}
        for pdf_file in pdf_files:
            filename = os.path.splitext(os.path.basename(pdf_file))[0]

            # 개별 PDF 파일 파싱
            documents = SimpleDirectoryReader(
                input_files=[pdf_file],
                file_extractor=file_extractor,
            ).load_data()

            result_dict[filename] = documents
            print(f"'{filename}.pdf' 파일 파싱 완료: {len(documents)} 페이지")

        return result_dict

    except PermissionError:
        print(f"'{folder_path}' 폴더에 접근할 권한이 없습니다.")
        return {}
    except Exception as e:
        print(f"PDF 파일 로드 중 오류가 발생했습니다: {str(e)}")
        return {}


def merge_documents_to_single_markdown(
    documents, output_folder, filename="merged_document"
):
    """
    PDF 문서 리스트를 하나의 Markdown 파일로 병합하여 저장하는 함수
    첫 번째 마크다운 파일을 제외한 모든 마크다운 파일의 첫 번째 줄을 제거

    Args:
        documents: PDF 문서 객체 리스트
        output_folder: Markdown 파일을 저장할 폴더 경로
        filename: 저장할 파일명 (확장자 제외)
    """
    import os

    # 출력 폴더가 없으면 생성
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # 병합된 Markdown 파일 경로
    merged_filepath = os.path.join(output_folder, f"{filename}.md")

    # 병합된 내용을 저장할 변수
    merged_content = ""

    # 각 문서(페이지)를 순회하며 내용 병합
    for i, doc in enumerate(documents):
        current_text = doc.text

        # 첫 번째 문서가 아닌 경우, 첫 번째 줄 제거
        if i > 0 and current_text:
            # 텍스트를 줄 단위로 분리
            lines = current_text.split("\n\n")

            # 첫 번째 줄이 있으면 제거
            if len(lines) > 0:
                current_text = "\n".join(lines[1:])

        # 병합된 내용에 현재 페이지 내용 추가
        current_text = current_text + " "
        merged_content += current_text

    # 병합된 내용을 파일로 저장
    with open(merged_filepath, "w", encoding="utf-8") as f:
        f.write(merged_content)

    print(f"모든 문서가 {merged_filepath}에 병합되어 저장되었습니다.")


def api_key_load():

    try:
        load_dotenv()
        LLAMA_CLOUD_API_KEY = os.getenv("LLAMA_CLOUD_API_KEY")

        if not LLAMA_CLOUD_API_KEY:
            raise ValueError("LLAMA_CLOUD_API_KEY가 환경 변수에 설정되지 않았습니다.")

        nest_asyncio.apply()

        print(f"API 키 로드 완료: {LLAMA_CLOUD_API_KEY[:5]}...")
        return LLAMA_CLOUD_API_KEY
    except Exception as e:
        raise ValueError(f"API 키 로드 중 오류 발생: {str(e)}")


def run():
    # 수정된 pdf_file_load 함수 호출
    pdf_documents = pdf_file_load("data")

    # 각 PDF 파일별로 처리
    for filename, documents in pdf_documents.items():
        if documents:
            # 각 PDF 파일의 모든 페이지를 하나의 마크다운으로 병합
            merge_documents_to_single_markdown(documents, "pdf2markdown", filename)
            print(f"'{filename}.pdf'를 마크다운으로 변환 완료")

    print("모든 PDF 파일이 개별 마크다운으로 변환되었습니다.")


if __name__ == "__main__":
    run()
