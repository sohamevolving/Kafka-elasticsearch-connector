from flask import Flask, jsonify
from elasticsearch import Elasticsearch

# Initialize Flask app and Elasticsearch client
app = Flask(__name__)
es = Elasticsearch(['http://localhost:9200'])  # Replace with your Elasticsearch server URL

# Route to get full index data
@app.route('/get_index_data/<index_name>', methods=['GET'])
def get_index_data(index_name):
    try:
        # Search the index to retrieve all documents (size=10000 is max, adjust accordingly)
        result = es.search(index=index_name, body={"query": {"match_all": {}}}, size=10000)
        # Extract hits (the actual documents)
        documents = result['hits']['hits']
        print(documents)
        return jsonify(documents), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Run the app
if __name__ == '__main__':
    app.run(debug=True)
  
