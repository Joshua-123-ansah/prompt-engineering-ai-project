# PDF Metadata Extraction Pipeline Comparison

## Project Overview

This project compares three different approaches for extracting metadata from academic PDF papers:
1. **GROBID** - A specialized tool for extracting structured information from academic documents
2. **NLP/ML** - A local natural language processing approach using spaCy
3. **LLM** - An AI-powered approach using OpenAI's language models

The goal is to evaluate which method performs best at extracting key information such as:
- Paper titles
- Author names and positions
- Affiliations
- Abstracts
- Total number of authors

All three pipelines are tested against a "gold standard" dataset to measure accuracy, precision, and recall.

## Project Structure

```
Project Promt Engineering/
├── grobid_extraction_v4.py      # GROBID extraction pipeline
├── NPL_extraction_pipeline.py   # NLP/ML extraction pipeline
├── LLM_extraction_pipeline.py   # LLM extraction pipeline
├── compare_pipelines.py         # Comparison and evaluation script
├── Golden Standard.xlsx         # Gold standard dataset for evaluation
├── My Papers/                   # Folder containing PDF files to process
├── requirements.txt             # Python package dependencies
└── README.md                    # This file
```

## Prerequisites

Before running this project, you need:

1. **Python 3.9 or higher** - Check if installed: `python3 --version`
2. **Docker** (optional) - Only needed if you want to run GROBID server
   - Check if installed: `docker --version`
   - Download: https://www.docker.com/products/docker-desktop
3. **PDF files** - Place your PDF papers in the `My Papers/` folder
4. **Gold Standard Excel file** - The `Golden Standard.xlsx` file with correct metadata
5. **OpenAI API Key** (optional) - Only needed if running the LLM pipeline
   - Set as environment variable: `export OPENAI_API_KEY="your-key-here"`

## Setup Instructions

### Step 1: Create a Virtual Environment

The project uses a virtual environment to manage dependencies. Create it first:

**On macOS/Linux:**
```bash
python3 -m venv venv
```

**On Windows:**
```bash
python -m venv venv
```

### Step 2: Activate the Virtual Environment

After creating the virtual environment, activate it:

**On macOS/Linux:**
```bash
source venv/bin/activate
```

**On Windows:**
```bash
venv\Scripts\activate
```

You should see `(venv)` at the beginning of your command prompt when activated.

### Step 3: Install Dependencies

Install all required packages:

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

This will install all necessary packages including pandas, numpy, spacy, openai, and others.

### Step 4: Download spaCy Language Model

The NLP pipeline requires a spaCy language model. Download it:

```bash
python -m spacy download en_core_web_sm
```

### Step 5: Set Up GROBID Server

If you want to use GROBID for better extraction accuracy, you need to run a GROBID server. The easiest way is using Docker:

1. **Install Docker** (if not already installed): https://www.docker.com/products/docker-desktop

2. **Run GROBID with Docker:**
   ```bash
   docker run -d --name grobid -p 8070:8070 lfoppiano/grobid:0.8.1
   ```

3. **Verify GROBID is running:**
   ```bash
   curl http://localhost:8070/api/isalive
   ```
   You should see: `{"status":"ok"}`

For more detailed GROBID installation instructions, visit: https://grobid.readthedocs.io/en/latest/Install-Grobid/

**Note**: If GROBID server is not running, the pipeline will automatically use a fallback PDF extraction method. This works but may be less accurate than using GROBID.

## How to Run the Project

### Option 1: Run Everything Automatically (Recommended)

This will run all three pipelines and then compare them:

```bash
# Run GROBID and NLP pipelines (no cost)
python compare_pipelines.py --run --run-which grobid,nlp

# Run ALL pipelines including LLM (requires API key and --llm-ok flag)
python compare_pipelines.py --run --llm-ok
```

### Option 2: Run Pipelines Individually

If you prefer to run each pipeline separately:

```bash
# 1. Run GROBID pipeline
python grobid_extraction_v4.py

# 2. Run NLP pipeline
python NPL_extraction_pipeline.py

# 3. Run LLM pipeline (requires OPENAI_API_KEY)
python LLM_extraction_pipeline.py

# 4. Compare all results
python compare_pipelines.py
```

### Option 3: Compare Existing Results

If you already have results from previous runs, you can just compare them:

```bash
python compare_pipelines.py
```

## Understanding the Output

### Pipeline Output Files

Each pipeline creates an Excel file with extracted metadata:
- `grobid_results_v4.xlsx` - Results from GROBID pipeline
- `nlp_results.xlsx` - Results from NLP pipeline
- `llm_results.xlsx` - Results from LLM pipeline

### Comparison Report

The comparison script generates a comprehensive report showing:

#### 1. Coverage
- Shows what percentage of papers from the gold standard were found by each pipeline
- Example: "GROBID found 85% of papers, NLP found 78%, LLM found 92%"

#### 2. Overall Metrics (Macro & Micro)
- **Precision**: How many of the extracted items were correct
- **Recall**: How many of the correct items were found
- **F1 Score**: A balanced measure combining precision and recall
- **Macro**: Average across all fields
- **Micro**: Overall across all items

#### 3. Per-Field Metrics
- Shows precision, recall, and F1 for each field:
  - Title
  - Target Author
  - Found Author Name
  - Author's Position
  - Total Authors
  - Affiliation
  - Abstract

#### 4. Time & Cost
- **Time**: How long each pipeline took to process all PDFs
- **Cost**: Estimated cost (mainly for LLM pipeline which uses API calls)

#### 5. Summary Sentences
- Automatic narrative summaries explaining which pipeline performed best

### Excel Export

The comparison also creates `pipeline_evaluation_summary.xlsx` with multiple sheets:
- **Coverage**: Coverage statistics
- **GROBID_per_field**: Detailed metrics for GROBID
- **NLP_per_field**: Detailed metrics for NLP
- **LLM_per_field**: Detailed metrics for LLM
- **SideBySide_PerField**: Comparison across all pipelines
- **Overall_MacroMicro**: Overall performance metrics
- **Time_Cost**: Runtime and cost information

## Interpreting Results

### What Makes a Good Result?

- **High Coverage**: Pipeline found most papers from the gold standard
- **High Precision**: Most extracted information is correct
- **High Recall**: Pipeline found most of the correct information
- **High F1 Score**: Good balance between precision and recall

### Example Interpretation

If you see:
```
GROBID: Precision=0.92, Recall=0.85, F1=0.88
NLP:    Precision=0.78, Recall=0.82, F1=0.80
LLM:    Precision=0.95, Recall=0.90, F1=0.92
```

This means:
- **LLM** has the best overall performance (highest F1)
- **GROBID** is second best
- **NLP** is third, but still performs reasonably well

## Configuration Options

### Changing Input Folder

By default, pipelines look for PDFs in `My Papers/`. To use a different folder:

```bash
python grobid_extraction_v4.py /path/to/your/pdfs
```

### Changing Target Authors

Edit the `TARGET_AUTHORS` list in each pipeline script, or pass via command line:

```bash
python grobid_extraction_v4.py "My Papers" "Author1, Author2, Author3"
```

### Environment Variables

You can customize behavior using environment variables:

```bash
# Change GROBID server URL (if running on different machine)
export GROBID_URL="http://your-server:8070"

# Change output file names
export OUTPUT_XLSX="custom_results.xlsx"

# Change OpenAI model
export OPENAI_MODEL="gpt-4o"
```

## Troubleshooting

### Error: "Missing input: grobid_results_v4.xlsx"

**Solution**: Run the pipelines first to generate the Excel files, or use `--run` flag:
```bash
python compare_pipelines.py --run --run-which grobid,nlp
```

### Error: "GROBID reachable at http://localhost:8070: NO"

**Solution**: This is normal if GROBID server isn't running. The script will use a fallback PDF extraction method. To use GROBID:

1. **Install Docker** (if not already installed): https://www.docker.com/products/docker-desktop

2. **Run GROBID using Docker:**
   ```bash
   docker run -d --name grobid -p 8070:8070 lfoppiano/grobid:0.8.1
   ```

3. **Verify it's running:**
   ```bash
   curl http://localhost:8070/api/isalive
   ```

For detailed GROBID installation and setup instructions, visit: https://grobid.readthedocs.io/en/latest/Install-Grobid/

**Alternative**: You can continue without GROBID - the pipeline will use a fallback method that works but may be less accurate.

### Error: "OpenAI available: NO"

**Solution**: Set your OpenAI API key:
```bash
export OPENAI_API_KEY="your-api-key-here"
```

### Time shows as NaN

**Solution**: This happens if pipelines were run before time tracking was added. Re-run the pipelines to get timing data.

## Technical Details

### Dependencies

- **pandas**: Data manipulation and Excel export
- **numpy**: Numerical computations
- **openpyxl**: Excel file handling
- **requests**: HTTP requests (for GROBID)
- **lxml**: XML parsing (for GROBID)
- **spacy**: Natural language processing
- **openai**: OpenAI API client
- **PyMuPDF/pdfminer**: PDF text extraction
- **tqdm**: Progress bars

### Performance Notes

- **GROBID**: Requires GROBID server running (or uses fallback)
- **NLP**: Runs locally, no external services needed
- **LLM**: Requires internet connection and API key, incurs costs

### File Formats

- Input: PDF files
- Output: Excel (.xlsx) files
- Gold Standard: Excel file with columns matching pipeline outputs

## Contact and Support

For questions or issues, please contact:

**Email**: jowusuan@asu.edu

You can also refer to the code comments for technical details.




