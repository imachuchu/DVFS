function(doc) {
    if (doc.doc_type == "dbFile" || doc.doc_type == "dbFolder")
        emit(doc.path, doc._id);
}
