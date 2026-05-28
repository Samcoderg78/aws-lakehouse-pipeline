# AWS Free Tier Deployment & Execution Plan
**Author**: Saurabh Yadav | **Project**: Syngenta AWS Lakehouse Pipeline

Follow this checklist step-by-step to deploy and run the project on your new AWS Free Tier account. This guide is optimized to keep your usage **100% Free** and completely inside the AWS Free Tier.

---

## 🛠️ Step 1: Configure AWS CLI with Your New Free Account
Before using Terraform, connect your local terminal to your new AWS account:

1. Log into your [AWS Management Console](https://console.aws.amazon.com/).
2. Search for **IAM** in the top search bar ➔ Go to **Users** ➔ Click **Create User**.
   * **User Name**: `saurabh-admin`
   * Select **Provide user access to the AWS Management Console** (Optional).
   * Click **Next**.
3. Under Permissions, select **Attach policies directly** and check **AdministratorAccess** (this lets you create the free S3, Glue, and Lambda resources via Terraform).
4. Click **Next** ➔ **Create User**.
5. Click on your new user `saurabh-admin` ➔ Go to **Security credentials** tab.
6. Scroll down to **Access keys** ➔ Click **Create access key**.
   * Select **Command Line Interface (CLI)**.
   * Click **Next** ➔ **Create access key**.
7. **Copy** your `Access Key ID` and `Secret Access Key`.
8. Open your local Windows PowerShell and configure the AWS CLI:
   ```powershell
   aws configure
   ```
   * Enter your **AWS Access Key ID**.
   * Enter your **AWS Secret Access Key**.
   * Set **Default region name**: `ap-south-1` (Mumbai region — closest to you!).
   * Set **Default output format**: `json`

Test your connection by running:
```powershell
aws sts get-caller-identity
```
*(If it returns your new account ID, you are successfully connected!)*

---

## ⚙️ Step 2: Initialize & Deploy via Terraform (100% Free Resources)

1. Open PowerShell and go to the `terraform` folder:
   ```powershell
   cd c:\Users\gaura\Downloads\Hackathon_syngenta\Syngenta-Hackathon-2026\aws-lakehouse-pipeline\terraform
   ```
2. Create a variables file so AWS knows where to send budget alerts:
   Create a new file named `terraform.tfvars` inside the `terraform` folder and add these lines:
   ```hcl
   aws_region   = "ap-south-1"
   environment  = "dev"
   alert_email  = "23f1003171@ds.study.iitm.ac.in"  # Your active email
   ```
3. Initialize Terraform:
   ```powershell
   terraform init
   ```
4. Perform a dry-run check:
   ```powershell
   terraform plan -out=tfplan
   ```
5. Apply the deployment (This provisions your S3 buckets, IAM roles, Glue jobs, and billing alarm):
   ```powershell
   terraform apply tfplan
   ```
   *(This takes ~2-3 minutes. Once finished, Terraform will print output variables. Copy the S3 bucket names and Topic ARN!)*

---

## 📧 Step 3: Accept the Budget & Alert Subscription
AWS will automatically send an email to confirm your free budget subscription:
1. Open your email inbox for `23f1003171@ds.study.iitm.ac.in`.
2. Find the email from **AWS Notifications** with the subject **AWS Notification - Subscription Confirmation**.
3. Open the email and click **Confirm Subscription**. 
*(This guarantees you get an email if your AWS account ever goes near $6, protecting you from any surprise bills!)*

---

## 📥 Step 4: Upload Test Data to Trigger the Pipeline
Now, upload the sample dataset to your S3 bucket. This triggers the entire pipeline automatically!

1. Open PowerShell and run the upload script (replace `<YOUR-ACCOUNT-ID>` with your actual AWS account ID, which was printed during `aws sts get-caller-identity`):
   ```powershell
   cd c:\Users\gaura\Downloads\Hackathon_syngenta\Syngenta-Hackathon-2026\aws-lakehouse-pipeline
   python scripts/upload_to_s3.py --bucket syngenta-lakehouse-dev-<YOUR-ACCOUNT-ID>
   ```
2. The upload will automatically trigger the S3 event, starting the **Step Functions State Machine** in the background.

---

## 👁️ Step 5: Monitor the Execution on AWS Console
1. Log into your [AWS Management Console](https://console.aws.amazon.com/).
2. Search for **Step Functions** ➔ Click on **State machines**.
3. Select `syngenta-lakehouse-dev-pipeline`.
4. You will see a live visual flowchart of your pipeline running! It will execute Bronze, Silver, and Gold steps. 
*(Each Glue job takes about 60 to 90 seconds. The whole pipeline completes in 2-3 minutes).*

---

## 📊 Step 6: Query Your Gold Data in Athena (Free Queries)
1. Go to **Amazon Athena** in the AWS Console.
2. Under **Workgroup** (top right), select `syngenta-lakehouse-dev-workgroup`.
3. In the left panel, select database `syngenta_lakehouse_dev_db`.
4. Open the SQL query file located at `sql/athena/analytics.sql` in your project and copy-paste any query into Athena to see the processed results, such as retailer scores and fungicide recommendations!

---

## 🛑 Step 7: Zero-Cost Clean-Up Checklist
To ensure your AWS account stays **100% Free** and you never pay a single rupee after finishing your learning/interviews, clean up with these steps:

1. Empty your S3 buckets (AWS does not allow Terraform to delete buckets that contain files):
   ```powershell
   aws s3 rm s3://syngenta-lakehouse-dev-<YOUR-ACCOUNT-ID> --recursive
   aws s3 rm s3://syngenta-lakehouse-dev-athena-results-<YOUR-ACCOUNT-ID> --recursive
   ```
2. Run Terraform destroy:
   ```powershell
   cd terraform
   terraform destroy
   ```
   *Type `yes` when prompted.*
3. This deletes every single resource we created (Glue, Lambda, Step Functions, Alarms, Roles). Your account is now completely empty and perfectly pristine!
