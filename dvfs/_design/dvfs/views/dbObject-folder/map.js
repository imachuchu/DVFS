function(doc) {
    if (doc.doc_type == "dbFile" || doc.doc_type == "dbFolder") {
        var pathAr = doc.path.split('/');
        emit(pathAr.splice(0, pathAr.length - 1).join('/') + '/', doc._id)
    }
}
