package schemas

import _ "embed"

//go:embed movie_event.avsc
var MovieEvent string

const MovieEventSubject = "movie-events-value"
