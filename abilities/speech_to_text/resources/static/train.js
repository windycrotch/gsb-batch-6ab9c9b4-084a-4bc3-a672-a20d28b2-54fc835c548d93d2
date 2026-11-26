let train = {
    init: function() {
        base.init({
            messageNames: train.messageNames,
            onLoad: train.onLoad,
            onMessage: train.onMessage,
        })
    },
    onLoad: function() {
        // Get references
        asticode.tools.sendHttp({
            method: "GET",
            url: "../routes/references/train",
            error: base.httpError,
            success: function(data) {
                // Handle cancel
                document.getElementById("btn-cancel").addEventListener("click", train.cancel)

                // Handle train
                document.getElementById("btn-train").addEventListener("click", train.train)

                // Create progresser
                train.progresser = asticode.progresser.new({
                    error: function(error) {
                        let e = document.getElementById("error")
                        e.innerText = error
                        e.style.display = "block"
                    },
                    root: document.getElementById("progress"),
                })

                // Init session from references
                train.session = 0
                train.updateProgress(data.responseJSON.progress)

                // Finish
                base.finish()
            }
        })
    },
    session: 0,
    ended: 0,
    messageNames: [
        "speech_to_text.progress",
    ],
    onMessage: function(data) {
        switch (data.name) {
            case "speech_to_text.progress":
                // Ignore progress from a session we don't know about
                if (typeof data.payload.session !== "undefined") {
                    if (data.payload.session < train.session) return
                }

                // Refresh error
                document.getElementById("error").style.display = "none"

                // Update progress
                train.updateProgress(data.payload)
                break
        }
    },
    train: function() {
        asticode.tools.sendHttp({
            method: "GET",
            url: "../routes/train",
            error: base.httpError,
        })
    },
    cancel: function() {
        asticode.tools.sendHttp({
            method: "GET",
            url: "../routes/train/cancel",
            error: base.httpError,
        })
    },
    updateProgress: function(progress) {
        // Get done
        let done = typeof progress === "undefined" || progress.done === true || typeof progress.error !== "undefined"

        // Handle session identity
        if (typeof progress !== "undefined" && typeof progress.session !== "undefined") {
            // Ignore progress from an older session
            if (progress.session < train.session) return

            // Ignore non terminal progress from a session that has already ended
            if (!done && progress.session <= train.ended) return

            // Handle new session
            if (progress.session > train.session) {
                // Reset the progresser so that a new session starts from scratch
                train.session = progress.session
                train.resetProgresser()
            }
        }

        // Session is over: show the train button again and clear the progresser
        if (done) {
            // Remember the session is over so that late callbacks can't revive it
            if (typeof progress !== "undefined" && typeof progress.session !== "undefined" && progress.session > train.ended) {
                train.ended = progress.session
            }

            document.getElementById("btn-cancel").style.display = "none"
            document.getElementById("btn-train").style.display = "block"

            // Display the error if any
            if (typeof progress !== "undefined" && typeof progress.error !== "undefined") {
                let e = document.getElementById("error")
                e.innerText = progress.error
                e.style.display = "block"
            }

            // Clear the progresser so that the previous session's percentage is never displayed again
            if (typeof progress !== "undefined") train.resetProgresser()
            return
        }

        // Hide/Show buttons
        document.getElementById("btn-cancel").style.display = "block"
        document.getElementById("btn-train").style.display = "none"

        // Update progresser
        if (typeof progress.steps !== "undefined" && progress.steps !== null && progress.steps.length > 0) {
            train.progresser.update(progress)
        }
    },
    resetProgresser: function() {
        train.progresser.reset()
        train.progresser.wrapper = undefined
    }
}
