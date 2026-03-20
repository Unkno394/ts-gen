import { useEffect, useMemo, useRef } from 'react';

type RGB = [number, number, number];
type HSL = { h: number; s: number; l: number };
type BackgroundColor = number | RGB;

type VibeBackgroundProps = {
  className?: string;
  collectionHue?: number;
  energy?: number;
  backgroundColor?: BackgroundColor;
  baseScale?: number;
  useDefaultHue?: boolean;
  customColors?: {
    top: HSL;
    middle: HSL;
    bottom: HSL;
  };
  playing?: boolean;
  lite?: boolean;
  staticFrame?: boolean;
};

const clamp = (v: number, min = 0, max = 1) => Math.min(max, Math.max(min, v));
const clampHue = (v: number) => clamp(v, 0, 360);
const randInt = (min: number, max: number) => Math.floor(Math.random() * (max - min + 1)) + min;
const withOffsetHue = (v: number) => (v + 280) % 360;
const keepHueRange = (v: number, ref: number) => (ref >= 280 && ref < 360 ? v % 360 : v);
const normalizeBackgroundColor = (color: BackgroundColor): RGB =>
  Array.isArray(color) ? color : [color, color, color];

const defaultCustomColors: { top: HSL; middle: HSL; bottom: HSL } = {
  top: { h: 278, s: 0.95, l: 0.62 },
  middle: { h: 245, s: 0.92, l: 0.55 },
  bottom: { h: 220, s: 0.88, l: 0.46 }
};

const defaultBackgroundColor: RGB = [0.01, 0.02, 0.05];

function hslToRgb(h: number, s = 1, l = 0.5): RGB {
  const a = s * Math.min(l, 1 - l);
  const f = (n: number) => {
    const k = ((n + h / 30) % 12 + 12) % 12;
    return l - a * Math.max(-1, Math.min(k - 3, 9 - k, 1));
  };
  return [f(0), f(8), f(4)];
}

class AnimatedValue {
  currentValue: number;
  targetValue: number;
  elapsedTime = 0;
  duration: number;

  constructor(value: number, duration: number) {
    this.currentValue = value;
    this.targetValue = value;
    this.duration = duration;
  }

  get value() {
    return this.currentValue;
  }

  update(next: number) {
    this.targetValue = next;
    this.elapsedTime = 0;
  }

  setImmediate(next: number) {
    this.currentValue = next;
    this.targetValue = next;
    this.elapsedTime = 0;
  }

  next(delta: number) {
    const t = clamp(this.elapsedTime / this.duration, 0, 1);
    this.elapsedTime += delta;
    this.currentValue = this.currentValue + (this.targetValue - this.currentValue) * t;
  }
}

class AnimatedColorPart {
  r: AnimatedValue;
  g: AnimatedValue;
  b: AnimatedValue;

  constructor(hue: number) {
    const [r, g, b] = hslToRgb(hue, 1, 0.5);
    this.r = new AnimatedValue(r, 3000);
    this.g = new AnimatedValue(g, 3000);
    this.b = new AnimatedValue(b, 3000);
  }

  get value(): RGB {
    return [this.r.value, this.g.value, this.b.value];
  }

  update(h: number, s?: number, l?: number) {
    const [r, g, b] = hslToRgb(h, s ?? 1, l ?? 0.5);
    this.r.update(r);
    this.g.update(g);
    this.b.update(b);
  }

  setImmediate(h: number, s?: number, l?: number) {
    const [r, g, b] = hslToRgb(h, s ?? 1, l ?? 0.5);
    this.r.setImmediate(r);
    this.g.setImmediate(g);
    this.b.setImmediate(b);
  }

  next(delta: number) {
    this.r.next(delta);
    this.g.next(delta);
    this.b.next(delta);
  }
}

class PaletteController {
  useDefaultHue = false;
  topStart: AnimatedColorPart;
  topEnd: AnimatedColorPart;
  middleStart: AnimatedColorPart;
  middleEnd: AnimatedColorPart;
  bottomStart: AnimatedColorPart;
  bottomEnd: AnimatedColorPart;

  constructor(hue = 10) {
    const [bottomStart, middleStart, topStart, bottomEnd, middleEnd, topEnd] = this.createParts(hue);
    this.topStart = topStart;
    this.topEnd = topEnd;
    this.middleStart = middleStart;
    this.middleEnd = middleEnd;
    this.bottomStart = bottomStart;
    this.bottomEnd = bottomEnd;
  }

  private createParts(hue: number): AnimatedColorPart[] {
    const primary = withOffsetHue(hue);
    const secondary = keepHueRange(primary + randInt(30, 40), primary);
    return [
      new AnimatedColorPart(primary),
      new AnimatedColorPart(300),
      new AnimatedColorPart(50),
      new AnimatedColorPart(secondary),
      new AnimatedColorPart(320),
      new AnimatedColorPart(50)
    ];
  }

  get value() {
    if (this.useDefaultHue) {
      return this.createParts(10).map((part) => part.value);
    }
    return [
      this.bottomStart.value,
      this.middleStart.value,
      this.topStart.value,
      this.bottomEnd.value,
      this.middleEnd.value,
      this.topEnd.value
    ];
  }

  switchToDefaultHue(flag: boolean) {
    this.useDefaultHue = flag;
  }

  setImmediateColors(colors: { top: HSL; middle: HSL; bottom: HSL }) {
    const { top, middle, bottom } = colors;
    this.topStart.setImmediate(clampHue(top.h), clamp(top.s), clamp(top.l));
    this.topEnd.setImmediate(clampHue(top.h), clamp(top.s), clamp(top.l));
    this.middleStart.setImmediate(clampHue(middle.h), clamp(middle.s), clamp(middle.l));
    this.middleEnd.setImmediate(clampHue(middle.h), clamp(middle.s), clamp(middle.l));
    this.bottomStart.setImmediate(clampHue(bottom.h), clamp(bottom.s), clamp(bottom.l));
    this.bottomEnd.setImmediate(clampHue(bottom.h), clamp(bottom.s), clamp(bottom.l));
  }

  next(delta: number) {
    this.topStart.next(delta);
    this.topEnd.next(delta);
    this.middleStart.next(delta);
    this.middleEnd.next(delta);
    this.bottomStart.next(delta);
    this.bottomEnd.next(delta);
  }
}

const VERTEX_SHADER = `
precision highp float;
attribute vec4 position;
void main() {
  gl_Position = position;
}
`;

const makeFragmentShader = (layers: 2 | 3) => `
precision highp float;
uniform vec2 vScreenSize;
uniform float vTime;
uniform float vScale;
uniform vec3 vColorBackground;
uniform vec3 vColor[6];
uniform vec3 vRotation[3];
uniform float vAudio[3];
uniform float vReact[3];
#define CIRCLE_WIDTH_BASE 0.8
#define CIRCLE_WIDTH_STEP 0.2
#define SPARK_STRENGTH_BASE 1.0
#define SPARK_STRENGTH_STEP 0.3
#define CIRCLE_RADIUS_BASE 0.95
#define CIRCLE_RADIUS_STEP 0.15
#define CIRCLE_OFFSET_BASE 0.0
#define CIRCLE_OFFSET_STEP 1.57
vec4 permute(vec4 x){return mod(((x*34.0)+1.0)*x, 289.0);}
vec4 taylorInvSqrt(vec4 r){return 1.79284291400159 - 0.85373472095314 * r;}
float snoise3(vec3 v) {
  const vec2 C = vec2(0.1666667, 0.3333333);
  const vec4 D = vec4(0.0, 0.5, 1.0, 2.0);
  vec3 i = floor(v + dot(v, C.yyy));
  vec3 x0 = v - i + dot(i, C.xxx);
  vec3 g = step(x0.yzx, x0.xyz);
  vec3 l = 1.0 - g;
  vec3 i1 = min(g.xyz, l.zxy);
  vec3 i2 = max(g.xyz, l.zxy);
  vec3 x1 = x0 - i1 + 1.0 * C.xxx;
  vec3 x2 = x0 - i2 + 2.0 * C.xxx;
  vec3 x3 = x0 - 1.0 + 3.0 * C.xxx;
  i = mod(i, 289.0);
  vec4 p = permute(permute(permute(i.z + vec4(0.0, i1.z, i2.z, 1.0)) + i.y + vec4(0.0, i1.y, i2.y, 1.0)) + i.x + vec4(0.0, i1.x, i2.x, 1.0));
  float n_ = 0.142857142857;
  vec3 ns = n_ * D.wyz - D.xzx;
  vec4 j = p - 49.0 * floor(p * ns.z * ns.z);
  vec4 x_ = floor(j * ns.z);
  vec4 y_ = floor(j - 7.0 * x_);
  vec4 x = x_ * ns.x + ns.yyyy;
  vec4 y = y_ * ns.x + ns.yyyy;
  vec4 h = 1.0 - abs(x) - abs(y);
  vec4 b0 = vec4(x.xy, y.xy);
  vec4 b1 = vec4(x.zw, y.zw);
  vec4 s0 = floor(b0) * 2.0 + 1.0;
  vec4 s1 = floor(b1) * 2.0 + 1.0;
  vec4 sh = -step(h, vec4(0.0));
  vec4 a0 = b0.xzyw + s0.xzyw * sh.xxyy;
  vec4 a1 = b1.xzyw + s1.xzyw * sh.zzww;
  vec3 p0 = vec3(a0.xy, h.x);
  vec3 p1 = vec3(a0.zw, h.y);
  vec3 p2 = vec3(a1.xy, h.z);
  vec3 p3 = vec3(a1.zw, h.w);
  vec4 norm = taylorInvSqrt(vec4(dot(p0,p0), dot(p1,p1), dot(p2,p2), dot(p3,p3)));
  p0 *= norm.x;
  p1 *= norm.y;
  p2 *= norm.z;
  p3 *= norm.w;
  vec4 m = max(0.6 - vec4(dot(x0,x0), dot(x1,x1), dot(x2,x2), dot(x3,x3)), 0.0);
  m = m * m;
  return 42.0 * dot(m * m, vec4(dot(p0,x0), dot(p1,x1), dot(p2,x2), dot(p3,x3)));
}
float tri(in float x){return abs(fract(x)-.5);}
vec3 tri3(in vec3 p){return vec3(tri(p.z+tri(p.y*20.)), tri(p.z+tri(p.x*1.)), tri(p.y+tri(p.x*1.)));}
float triNoise3D(in vec3 p, in float spd) {
  float z = 0.4;
  float rz = 0.1;
  vec3 bp = p;
  for (float i = 0.; i <= 4.; i++) {
    vec3 dg = tri3(bp * 0.01);
    p += (dg + vTime * 0.1 * spd);
    bp *= 4.;
    z *= 0.9;
    p *= 1.6;
    rz += (tri(p.z + tri(0.6 * p.x + 0.1 * tri(p.y)))) / z;
  }
  return smoothstep(0.0, 8., rz + sin(rz + sin(z) * 2.8) * 2.2);
}
vec2 rotate(vec2 p, float a) {
  float s = sin(a);
  float c = cos(a);
  return vec2(p.x * c - p.y * s, p.x * s + p.y * c);
}
float light(float intensity, float attenuation, float dist) {
  return intensity / (1.0 + dist + dist * attenuation);
}
vec4 makeNoiseBlob2(vec2 uv, vec3 color1, vec3 color2, float strength, float offset) {
  float len = length(uv);
  float v0, v1;
  float r0, d0, n0;
  n0 = snoise3(vec3(uv * 1.2 + offset, vTime * 0.5 + offset)) * 0.5 + 0.5;
  r0 = mix(0.0, 1.0, n0);
  d0 = distance(uv, r0 / len * uv);
  v0 = smoothstep(r0 + 0.1 + (sin(vTime + offset) + 1.0), r0, len);
  v1 = light(0.08 * (1.0 + 1.2 * (-sin(vTime * 2.0 + offset * 0.5) * 0.5)) + 0.14 * strength, 14.0, d0);
  vec3 col = mix(color1, color2, uv.y * 2.);
  col = col + v1;
  col.rgb = clamp(col.rgb, 0.0, 1.0);
  return vec4(col, v0);
}
vec4 makeBlob(vec2 uv, float blob, vec3 color1, vec3 color2, float width, float baseReaction, float likeReaction, float audioStrength, float offset, vec2 noiseOffset) {
  float len = length(uv);
  float outerRadius = blob + width * 0.5 + baseReaction * (1.0 + max(likeReaction, audioStrength * 0.6) * 50. * baseReaction);
  float strength = max(likeReaction, audioStrength);
  vec4 noise = makeNoiseBlob2(uv * (1.0 - likeReaction * 0.5) + noiseOffset, color1, color2, strength, offset);
  noise.a = mix(0.0, noise.a, smoothstep(outerRadius, 0.5, len));
  noise.rgb += 0.18 * likeReaction * (1.0 - smoothstep(0.2, outerRadius * 0.8, len));
  return noise;
}
void main() {
  vec2 uv = gl_FragCoord.xy / vScreenSize.xy;
  uv = uv * 2.0 - 1.0;
  uv.y *= vScreenSize.y / min(vScreenSize.x, vScreenSize.y) / vScale;
  uv.x *= vScreenSize.x / min(vScreenSize.x, vScreenSize.y) / vScale;
  vec2 ruv = uv * 2.0;
  float pa = atan(ruv.y, ruv.x);
  float idx = (pa / 3.1415) / 2.0;
  vec2 ruv1 = rotate(uv * 2.0, 3.1415);
  float pa1 = atan(ruv1.y, ruv1.x);
  float idx1 = (pa1 / 3.1415) / 2.0;
  float idx21 = (pa1 / 3.1415 + 1.0) / 2.0 * 3.1415;
  float spark = triNoise3D(vec3(idx, 0.0, 0.0), 0.1);
  spark = mix(spark, triNoise3D(vec3(idx1, 0.0, idx1), 0.1), smoothstep(0.9, 1.0, sin(idx21)));
  spark = spark * 0.08 + pow(spark, 7.0);
  spark = smoothstep(0.0, spark, 0.3) * spark;
  vec3 color = vColorBackground;
  vec4 blobColor;
  float floatIndex;
  float radius;
  float n0 = snoise3(vec3(uv * 1.2, vTime * 0.5));
  for (int i = 0; i < ${layers}; i++) {
    floatIndex = float(i);
    radius = CIRCLE_RADIUS_BASE - CIRCLE_RADIUS_STEP * floatIndex;
    blobColor = makeBlob(
      uv,
      mix(radius, radius + 0.3, n0),
      vColor[i],
      vColor[i + 3],
      CIRCLE_WIDTH_BASE - CIRCLE_WIDTH_STEP * floatIndex,
      (SPARK_STRENGTH_BASE - SPARK_STRENGTH_STEP * floatIndex) * spark,
      vReact[i],
      vAudio[i],
      CIRCLE_OFFSET_BASE + CIRCLE_OFFSET_STEP * floatIndex,
      rotate(vRotation[i].xy, vTime * vRotation[i].z)
    );
    color = mix(color, blobColor.rgb, blobColor.a);
  }
  gl_FragColor = vec4(color, 1.0);
}
`;

function compileShader(gl: WebGLRenderingContext, type: number, source: string) {
  const shader = gl.createShader(type);
  if (!shader) throw new Error('Failed to create shader');
  gl.shaderSource(shader, source);
  gl.compileShader(shader);
  if (!gl.getShaderParameter(shader, gl.COMPILE_STATUS)) {
    const info = gl.getShaderInfoLog(shader) || 'Shader compile error';
    gl.deleteShader(shader);
    throw new Error(info);
  }
  return shader;
}

function createProgram(gl: WebGLRenderingContext, vertexSource: string, fragmentSource: string) {
  const vs = compileShader(gl, gl.VERTEX_SHADER, vertexSource);
  const fs = compileShader(gl, gl.FRAGMENT_SHADER, fragmentSource);
  const program = gl.createProgram();
  if (!program) throw new Error('Failed to create program');
  gl.attachShader(program, vs);
  gl.attachShader(program, fs);
  gl.linkProgram(program);
  if (!gl.getProgramParameter(program, gl.LINK_STATUS)) {
    const info = gl.getProgramInfoLog(program) || 'Program link error';
    gl.deleteProgram(program);
    gl.deleteShader(vs);
    gl.deleteShader(fs);
    throw new Error(info);
  }
  gl.deleteShader(vs);
  gl.deleteShader(fs);
  return program;
}

export function VibeBackground({
  className,
  collectionHue = 12,
  energy = 0.22,
  backgroundColor = defaultBackgroundColor,
  baseScale = 0.72,
  useDefaultHue = false,
  customColors = defaultCustomColors,
  playing = false,
  lite = false,
  staticFrame = false
}: VibeBackgroundProps) {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);

  const stableProps = useMemo(
    () => ({ collectionHue, energy, backgroundColor, baseScale, useDefaultHue, customColors, playing, lite, staticFrame }),
    [collectionHue, energy, backgroundColor, baseScale, useDefaultHue, customColors, playing, lite, staticFrame]
  );

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;

    const gl = canvas.getContext('webgl', {
      alpha: false,
      antialias: false,
      premultipliedAlpha: false,
      preserveDrawingBuffer: false,
      depth: true,
      stencil: false,
      powerPreference: 'default'
    });

    if (!gl) return;

    const fragmentShader = makeFragmentShader(stableProps.lite ? 2 : 3);
    const program = createProgram(gl, VERTEX_SHADER, fragmentShader);

    const positionLoc = gl.getAttribLocation(program, 'position');
    const vScreenSize = gl.getUniformLocation(program, 'vScreenSize');
    const vTime = gl.getUniformLocation(program, 'vTime');
    const vScale = gl.getUniformLocation(program, 'vScale');
    const vColorBackground = gl.getUniformLocation(program, 'vColorBackground');
    const vColor = gl.getUniformLocation(program, 'vColor');
    const vRotation = gl.getUniformLocation(program, 'vRotation');
    const vAudio = gl.getUniformLocation(program, 'vAudio');
    const vReact = gl.getUniformLocation(program, 'vReact');

    const positionBuffer = gl.createBuffer();
    if (!positionBuffer) return;

    gl.bindBuffer(gl.ARRAY_BUFFER, positionBuffer);
    gl.bufferData(
      gl.ARRAY_BUFFER,
      new Float32Array([-1, -1, 1, -1, -1, 1, -1, 1, 1, -1, 1, 1]),
      gl.STATIC_DRAW
    );

    const palette = new PaletteController(stableProps.collectionHue);
    palette.switchToDefaultHue(Boolean(stableProps.useDefaultHue));
    const bgColor = normalizeBackgroundColor(stableProps.backgroundColor);
    palette.setImmediateColors(stableProps.customColors);

    const energyValue = new AnimatedValue(0.2, 1000);
    const audioRatio = new AnimatedValue(0, 1000);
    const reactTop = new AnimatedValue(0, 600);
    const reactMiddle = new AnimatedValue(0, 600);
    const reactBottom = new AnimatedValue(0, 600);

    energyValue.update(0.4 * (stableProps.energy + 1));
    audioRatio.update(stableProps.playing ? 1 : 0);

    const rotations = new Float32Array([-0.3, 0.3, 0.2, -0.3, -0.3, -0.2, -0.3, -0.3, 0.2]);
    const flatColors = new Float32Array(18);
    const audio = new Float32Array(3);
    const reacts = new Float32Array(3);

    let audioLow = 0;
    let audioMiddle = 0;
    let audioHigh = 0;
    let raf = 0;
    let active = true;
    const fps = 25;
    const frameDuration = 1000 / fps;
    let last = performance.now();
    let accum = 0;
    let viewportWidth = 0;
    let viewportHeight = 0;
    let viewportScale = 1;

    const prefersReducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    const deviceMemory = (navigator as Navigator & { deviceMemory?: number }).deviceMemory;
    const lowMemory = typeof deviceMemory === 'number' ? deviceMemory <= 4 : false;
    const isWeakDevice = prefersReducedMotion || lowMemory || (navigator.hardwareConcurrency ? navigator.hardwareConcurrency <= 4 : false) || window.innerWidth <= 480;
    let timeValue = isWeakDevice ? 420 : Math.floor(3600 * Math.random());

    const getUpdatedAudioParam = (current: number, next: number, step = 0.02) => {
      const a = clamp(current);
      const b = clamp(next);
      if (b > a) return b;
      if (b < a) return a - Math.min(step, a - b);
      return a;
    };

    const updateCanvasSize = () => {
      const isMicro = window.innerWidth <= 320;
      const isTiny = window.innerWidth <= 375;
      const isNarrow = window.innerWidth <= 425;
      const isMobile = window.innerWidth < 768;
      const isCompact = window.innerWidth <= 900;
      const dpr = Math.min(window.devicePixelRatio || 1, 2);
      const width = Math.round(window.innerWidth * dpr);
      const height = Math.round(window.innerHeight * dpr);
      viewportWidth = width;
      viewportHeight = height;
      viewportScale = (isMicro ? 1.14 : isTiny ? 1.08 : isNarrow ? 1.02 : isMobile ? 0.96 : isCompact ? 0.92 : 0.88) * stableProps.baseScale;
      if (canvas.width !== width || canvas.height !== height) {
        canvas.width = width;
        canvas.height = height;
        gl.viewport(0, 0, width, height);
      }
    };

    const drawFrame = () => {
      const paletteValues = palette.value;
      let ptr = 0;
      for (let i = 0; i < paletteValues.length; i++) {
        flatColors[ptr++] = paletteValues[i][0];
        flatColors[ptr++] = paletteValues[i][1];
        flatColors[ptr++] = paletteValues[i][2];
      }
      audio[0] = audioLow;
      audio[1] = audioMiddle;
      audio[2] = audioHigh;
      reacts[0] = reactTop.value;
      reacts[1] = reactMiddle.value;
      reacts[2] = reactBottom.value;
      gl.clearColor(bgColor[0], bgColor[1], bgColor[2], 1);
      gl.clear(gl.COLOR_BUFFER_BIT);
      gl.disable(gl.BLEND);
      gl.useProgram(program);
      gl.bindBuffer(gl.ARRAY_BUFFER, positionBuffer);
      gl.enableVertexAttribArray(positionLoc);
      gl.vertexAttribPointer(positionLoc, 2, gl.FLOAT, false, 0, 0);
      gl.uniform2f(vScreenSize, viewportWidth, viewportHeight);
      gl.uniform1f(vTime, timeValue);
      gl.uniform1f(vScale, viewportScale);
      gl.uniform3f(vColorBackground, bgColor[0], bgColor[1], bgColor[2]);
      gl.uniform3fv(vColor, flatColors);
      gl.uniform3fv(vRotation, rotations);
      gl.uniform1fv(vAudio, audio);
      gl.uniform1fv(vReact, reacts);
      gl.drawArrays(gl.TRIANGLES, 0, 6);
    };

    const render = (now: number) => {
      if (!active) return;
      const delta = now - last;
      last = now;
      accum += delta;
      if (accum >= frameDuration - 0.1) {
        accum %= frameDuration;
        energyValue.next(delta);
        audioRatio.next(delta);
        reactTop.next(delta);
        reactMiddle.next(delta);
        reactBottom.next(delta);
        palette.next(delta);
        timeValue = (timeValue + energyValue.value * delta / 1000) % 86400;
        audioLow = getUpdatedAudioParam(audioLow, 0) * audioRatio.value;
        audioMiddle = getUpdatedAudioParam(audioMiddle, 0) * audioRatio.value;
        audioHigh = getUpdatedAudioParam(audioHigh, 0) * audioRatio.value;
        drawFrame();
      }
      raf = requestAnimationFrame(render);
    };

    updateCanvasSize();
    if (isWeakDevice || stableProps.staticFrame) {
      drawFrame();
      const onResize = () => {
        updateCanvasSize();
        drawFrame();
      };
      window.addEventListener('resize', onResize);
      return () => {
        active = false;
        window.removeEventListener('resize', onResize);
        gl.deleteBuffer(positionBuffer);
        gl.deleteProgram(program);
      };
    }

    const onResize = () => updateCanvasSize();
    raf = requestAnimationFrame(render);
    window.addEventListener('resize', onResize);

    return () => {
      active = false;
      cancelAnimationFrame(raf);
      window.removeEventListener('resize', onResize);
      gl.deleteBuffer(positionBuffer);
      gl.deleteProgram(program);
    };
  }, [stableProps]);

  return (
    <div className={['vibe-root', className].filter(Boolean).join(' ')}>
      <canvas ref={canvasRef} className="vibe-canvas" />
    </div>
  );
}
