.PHONY: run run-container gcloud-deploy

run:
	@streamlit run 1_Penetration.py --server.port=8080 --server.address=0.0.0.0

run-container:
	@docker build . -t biloma
	@docker run -p 8080:8080 biloma

gcloud-deploy:
	@gcloud app deploy app.yaml
