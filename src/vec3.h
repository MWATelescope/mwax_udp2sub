/** Functions for 3-vectors.
 * Author: Luke Williams <luke.a.williams@curtin.edu.au>
 */
#ifndef __VEC3_H__
#define __VEC3_H__

#include <math.h>

typedef struct vec3 {
  long double x;
  long double y;
  long double z;
} vec3_t;

#define deg2rad(x) (x * M_PIl / 180L)

static inline long double vec3_magnitude(vec3_t v) {
  return sqrtl(v.x*v.x + v.y*v.y + v.z*v.z);
}

static inline vec3_t vec3_add(vec3_t a, vec3_t b) {
  return (vec3_t) { a.x + b.x, a.y + b.y, a.z + b.z };
}

static inline vec3_t vec3_subtract(vec3_t a, vec3_t b) {
  return (vec3_t) { a.x - b.x, a.y - b.y, a.z - b.z };
}

static inline vec3_t vec3_scale(long double k, vec3_t v) {
  return (vec3_t) { k*v.x, k*v.y, k*v.z };
}

static inline vec3_t vec3_normalise(vec3_t v) {
  return vec3_scale(1L / vec3_magnitude(v), v);
}

static inline long double vec3_dot(vec3_t a, vec3_t b) {
  return a.x * b.x + a.y * b.y + a.z * b.z;
}

static inline vec3_t vec3_unit(long double b, long double a) {
  return (vec3_t) { cosl(a)*cosl(b),  sinl(a)*cosl(b), sinl(b) };
}

#endif