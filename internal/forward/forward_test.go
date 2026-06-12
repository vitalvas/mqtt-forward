package forward

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		cases := []struct {
			name   string
			in     string
			listen string
			target string
		}{
			{"port_host_port", "8080:localhost:22", "localhost:8080", "localhost:22"},
			{"bind_port_host_port", "127.0.0.1:8080:db:5432", "127.0.0.1:8080", "db:5432"},
			{"bind_all_explicit", "0.0.0.0:9090:internal:443", "0.0.0.0:9090", "internal:443"},
			{"numeric_target_host", "5000:10.0.0.5:6000", "localhost:5000", "10.0.0.5:6000"},
			{"ephemeral_local_port", "0:localhost:22", "localhost:0", "localhost:22"},
			{"ipv6_target", "8080:[2001:db8::1]:443", "localhost:8080", "[2001:db8::1]:443"},
			{"ipv6_loopback_target", "2222:[::1]:22", "localhost:2222", "[::1]:22"},
			{"ipv6_bind", "[::1]:8080:db:5432", "[::1]:8080", "db:5432"},
			{"ipv6_bind_and_target", "[::1]:8080:[2001:db8::1]:443", "[::1]:8080", "[2001:db8::1]:443"},
			{"ipv6_unspecified_bind", "[::]:8080:localhost:22", "[::]:8080", "localhost:22"},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				spec, err := Parse(tc.in)
				require.NoError(t, err)
				assert.Equal(t, tc.listen, spec.Listen)
				assert.Equal(t, tc.target, spec.Target)
			})
		}
	})

	t.Run("omitted_bind_defaults_to_loopback", func(t *testing.T) {
		spec, err := Parse("8080:db:5432")
		require.NoError(t, err)
		assert.Equal(t, "localhost:8080", spec.Listen)
	})

	t.Run("explicit_bind_overrides_default", func(t *testing.T) {
		spec, err := Parse("0.0.0.0:8080:db:5432")
		require.NoError(t, err)
		assert.Equal(t, "0.0.0.0:8080", spec.Listen)
	})

	t.Run("invalid", func(t *testing.T) {
		cases := []struct {
			name string
			in   string
		}{
			{"empty", ""},
			{"single_field", "8080"},
			{"two_fields", "8080:localhost"},
			{"empty_target_host", "8080::22"},
			{"non_numeric_local_port", "abc:localhost:22"},
			{"non_numeric_target_port", "8080:localhost:ssh"},
			{"local_port_out_of_range", "70000:localhost:22"},
			{"target_port_out_of_range", "8080:localhost:70000"},
			{"zero_target_port", "8080:localhost:0"},
			{"empty_target_port", "8080:localhost:"},
			{"bare_ipv6_target", "8080:2001:db8::1:443"},
			{"empty_bracket_target", "8080:[]:443"},
			{"empty_bracket_bind", "[]:8080:localhost:22"},
			{"missing_local_port_with_ipv6_target", "[2001:db8::1]:443"},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := Parse(tc.in)
				assert.Error(t, err)
			})
		}
	})
}

func TestParseAll(t *testing.T) {
	t.Run("valid_multiple", func(t *testing.T) {
		specs, err := ParseAll([]string{"8080:localhost:22", "9090:db:5432"})
		require.NoError(t, err)
		require.Len(t, specs, 2)
		assert.Equal(t, "localhost:8080", specs[0].Listen)
		assert.Equal(t, "localhost:22", specs[0].Target)
		assert.Equal(t, "localhost:9090", specs[1].Listen)
		assert.Equal(t, "db:5432", specs[1].Target)
	})

	t.Run("mixed_ipv4_ipv6", func(t *testing.T) {
		specs, err := ParseAll([]string{"8080:localhost:22", "[::1]:9090:[2001:db8::1]:443"})
		require.NoError(t, err)
		require.Len(t, specs, 2)
		assert.Equal(t, "localhost:8080", specs[0].Listen)
		assert.Equal(t, "[::1]:9090", specs[1].Listen)
		assert.Equal(t, "[2001:db8::1]:443", specs[1].Target)
	})

	t.Run("empty", func(t *testing.T) {
		_, err := ParseAll(nil)
		assert.Error(t, err)
	})

	t.Run("propagates_parse_error", func(t *testing.T) {
		_, err := ParseAll([]string{"8080:localhost:22", "bad"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid forward")
	})

	t.Run("rejects_duplicate_listen", func(t *testing.T) {
		_, err := ParseAll([]string{"8080:localhost:22", "8080:db:5432"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate listen address")
	})

	t.Run("same_port_different_bind_is_allowed", func(t *testing.T) {
		specs, err := ParseAll([]string{"127.0.0.1:8080:localhost:22", "0.0.0.0:8080:db:5432"})
		require.NoError(t, err)
		assert.Len(t, specs, 2)
	})
}
