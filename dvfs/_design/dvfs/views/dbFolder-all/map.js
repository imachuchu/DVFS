function(doc) {
    if (doc.doc_type == 'dbFolder')
        emit(doc.path, doc);
}
