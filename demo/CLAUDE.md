# Development Notes

## API Requirements
- The suppress-data API should be PYTHON only
- Do not use child processes or subprocess calls from Node.js
- Use pure Python implementation with Vercel Python runtime

## Setup
- Install dependencies: `pip install duckdb` for Ostrich Egg engine
- Use `@vercel/python` runtime for Python API routes