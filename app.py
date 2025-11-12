from flask import Flask, render_template, request, session, send_file
import asyncio
from faculty_crawler_v2 import FastFacultyCrawlerV2
import pandas as pd
import io
import os
import nest_asyncio  # ✅ Needed to allow asyncio inside Flask on Render or Gunicorn

# Apply nest_asyncio patch once
nest_asyncio.apply()

app = Flask(__name__)
app.secret_key = 'supersecretkey'  # Needed for session management


async def run_crawler(keyword):
    """
    This function runs the crawler with the given keyword.
    """
    print(f"Crawling for keyword: {keyword}")
    urls = [
        "https://iitm.irins.org",
        "https://iith.irins.org",
        "https://iiti.irins.org",
        "https://iitp.irins.org",
        "https://iiscprofiles.irins.org",
        "https://iitk.irins.org",
        "https://iitd.irins.org",
        "https://iitr.irins.org",
        "https://iiserb.irins.org",
        "https://iittp.irins.org",
        "https://iisermohali.irins.org",
        "https://iitjammu.irins.org",
        "https://rrit.irins.org",
        "https://hri.irins.org",
        "https://iitbhilai.irins.org",
        "https://iitkgp.irins.org"
    ]
    crawler = FastFacultyCrawlerV2(base_urls=urls, max_concurrent_requests=100)
    results = await crawler.crawl(keyword=keyword)
    return results


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/search', methods=['POST'])
def search():
    try:
        keyword = request.form['keyword'].strip()
        search_type = request.form.get('search_type', 'keyword')
        institution = request.form.get('institution', 'all')

        print(f"[DEBUG] Search request - Type: {search_type}, Keyword: {keyword}, Institution: {institution}")

        try:
            if not keyword:
                search_query = None
            else:
                if search_type == 'name' and not keyword.lower().startswith('name:'):
                    search_query = f"name:{keyword}"
                elif search_type == 'vidwan' and not keyword.lower().startswith('vidwan:'):
                    search_query = f"vidwan:{keyword}"
                else:
                    search_query = keyword

            print(f"[DEBUG] Search query: {search_query}")

            # ✅ Run the async crawler properly under Flask
            loop = asyncio.get_event_loop()
            if loop.is_running():
                results = loop.create_task(run_crawler(search_query))
            else:
                results = loop.run_until_complete(run_crawler(search_query))

            if isinstance(results, asyncio.Task):
                results = loop.run_until_complete(results)

            print(f"[DEBUG] Found {len(results)} initial results")

            if institution != 'all':
                results = [r for r in results if r.get('Institution') == institution]
                print(f"[DEBUG] After institution filter: {len(results)} results")

            if not results:
                return render_template(
                    'results.html',
                    keyword=keyword,
                    results=[],
                    search_type=search_type,
                    institution=institution,
                    message="No matching profiles found."
                )

            session['results'] = results

            return render_template(
                'results.html',
                keyword=keyword,
                results=results,
                search_type=search_type,
                institution=institution
            )

        except Exception as e:
            print(f"[ERROR] Search execution error: {str(e)}")
            return render_template(
                'results.html',
                error=f"An error occurred during the search. Please try again later. Details: {str(e)}"
            )

    except Exception as e:
        print(f"[ERROR] Form processing error: {str(e)}")
        return render_template(
            'index.html',
            error="An error occurred while processing your request. Please try again."
        )


@app.route('/download')
def download():
    try:
        results = session.get('results', [])
        if not results:
            return "No results to download.", 404

        timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
        filename = f'faculty_data_{timestamp}.xlsx'

        clean_results = []
        for result in results:
            clean_result = result.copy()
            clean_result.pop('html_content', None)
            clean_results.append(clean_result)

        df = pd.DataFrame(clean_results)

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Faculty Profiles')
            worksheet = writer.sheets['Faculty Profiles']
            for idx, col in enumerate(df.columns):
                series = df[col]
                max_len = max(
                    (series.astype(str).map(len).max(), len(str(series.name)))
                ) + 2
                worksheet.column_dimensions[chr(65 + idx)].width = max_len

        output.seek(0)

        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )
    except Exception as e:
        print(f"Error in download: {str(e)}")
        return f"Error occurred while downloading: {str(e)}", 500


if __name__ == '__main__':
    # ✅ Use PORT environment variable for Render
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
