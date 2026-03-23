for file in raw_documents:

    # Step 1: classify
    scanned = classify(file)

    # Step 2: extract
    if scanned:
        data = run_ocr_pipeline(file)
    else:
        data = extract_pdf_tables(file)

    # Step 3: clean & structure
    data = parse_tables(data)

    # Step 4: hierarchy mapping
    data = apply_hierarchy(data)

    # Step 5: validation
    errors = validate(data)

    # Step 6: save
    save(data)

    # Step 7: metadata
    log_metadata(file, errors)