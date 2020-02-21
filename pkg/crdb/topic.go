package crdb

import (
	"fmt"
	"time"
	"regexp"

	"github.com/pkg/errors"
)

var disallowedTopicCharacters = regexp.MustCompile(`[^A-Za-z0-9\-\$\:\.\_]`)

var ErrInvalidTopicName = errors.New("topic name should not contain characters matched by " + disallowedTopicCharacters.String())

// validateTopicName checks if the topic name contains any characters which could be unsuitable for the SQL Pub/Sub.
// Topics are translated into SQL tables and patched into some queries, so this is done to prevent injection as well.
func validateTopicName(topic string) error {
	if disallowedTopicCharacters.MatchString(topic) {
		return errors.Wrap(ErrInvalidTopicName, topic)
	}

	return nil
}

func messageTable(topic string) string {
	return fmt.Sprintf(`"watermill_%s"`, topic)
}

func claimsTable(topic string, consumerGroup string) string {
	return fmt.Sprintf(`"watermill_%s_%s"`, topic, consumerGroup)
}

func ChangeFeedQuery(topic string, resolved time.Duration, cursor time.Time) string {
	query := fmt.Sprintf(
		`EXPERIMENTAL CHANGEFEED FOR %s WITH format = json, resolved = %s, envelope = key_only`,
		messageTable(topic),
		resolved.String(),
	)

	if !cursor.IsZero() {
		query = fmt.Sprintf(`%s, cursor = '%d'`, query, cursor.UnixNano())
	}

	return query
}
