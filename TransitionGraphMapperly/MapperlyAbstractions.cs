// Minimal vendored abstractions for Mapperly.
//
// Some environments might not have access to NuGet package "Riok.Mapperly.Abstractions".
// Mapperly's source generator only requires these attribute type names to exist.

using System;

namespace Riok.Mapperly.Abstractions
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
    public sealed class MapperAttribute : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Method, AllowMultiple = true, Inherited = false)]
    public sealed class MapperIgnoreTargetAttribute : Attribute
    {
        public MapperIgnoreTargetAttribute(string targetMemberName)
        {
            TargetMemberName = targetMemberName;
        }

        public string TargetMemberName { get; }
    }

    [AttributeUsage(AttributeTargets.Method, AllowMultiple = true, Inherited = false)]
    public sealed class MapperIgnoreSourceAttribute : Attribute
    {
        public MapperIgnoreSourceAttribute(string sourceMemberName)
        {
            SourceMemberName = sourceMemberName;
        }

        public string SourceMemberName { get; }
    }

    [AttributeUsage(AttributeTargets.Method, AllowMultiple = true, Inherited = false)]
    public sealed class MapPropertyAttribute : Attribute
    {
        public MapPropertyAttribute(string source, string target)
        {
            Source = source;
            Target = target;
        }

        public string Source { get; }
        public string Target { get; }
    }
}
