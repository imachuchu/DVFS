function(doc) {
    if (doc.doc_type == "dbFile")
        emit(doc.path, doc._id);
}
