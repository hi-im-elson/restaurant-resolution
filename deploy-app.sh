docker container rm streamlit-container
docker image rm streamlit-image 
docker build . -t streamlit-image && \
#docker run -p 8080:8080 --name streamlit-container streamlit-image && \
gcloud app deploy app.yaml
