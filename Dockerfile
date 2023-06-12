FROM public.ecr.aws/lambda/python:3.9 AS model
RUN curl -s https://packagecloud.io/install/repositories/github/git-lfs/script.rpm.sh | bash
RUN yum update -y
RUN yum install git -y
RUN yum install -y amazon-linux-extras
RUN PYTHON=python2 amazon-linux-extras install epel -y 
RUN yum-config-manager --enable epel
RUN yum install git-lfs -y 
RUN git lfs install
RUN git clone https://huggingface.co/sentence-transformers/all-mpnet-base-v2 /tmp/model
RUN rm -rf /tmp/model/.git

FROM public.ecr.aws/lambda/python:3.10
# Install the function's dependencies using file requirements.txt
# from your project folder.
COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"
# Copy function code
COPY app.py ${LAMBDA_TASK_ROOT}
COPY --from=model /tmp/model /tmp/sentence-transformers_all-mpnet-base-v2
# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "app.lambda_handler" ]