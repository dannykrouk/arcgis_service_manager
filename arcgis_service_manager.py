#!/usr/bin/env python3
"""
ArcGIS Server Service Manager

This script manages ArcGIS Server services through the REST API.
It can save current service states, stop services (except one), or restore services from a saved state.

Requirements:
    Use the Python that comes with ArcGIS Pro or ArcGIS Server (e.g. "c:\Program Files\ArcGIS\Server\framework\runtime\ArcGIS\bin\Python\Scripts\propy.bat")

Usage:
    python arcgis_service_manager.py save --server https://your-server:6443 --username admin --password yourpass --output services.csv
    python arcgis_service_manager.py stop --server https://your-server:6443 --username admin --password yourpass --keep-service "MyService"
    python arcgis_service_manager.py restore --server https://your-server:6443 --username admin --password yourpass --input services.csv
"""

import requests
import json
import csv
import argparse
import sys
import urllib.parse
from typing import Dict, List, Optional, Tuple


class ArcGISServerManager:
    def __init__(self, server_url: str, username: str, password: str):
        """
        Initialize the ArcGIS Server manager.
        
        Args:
            server_url: Base URL of the ArcGIS Server (e.g., https://server:6443)
            username: Primary Site Administrator username
            password: Primary Site Administrator password
        """
        self.server_url = server_url.rstrip('/')
        self.username = username
        self.password = password
        self.token = None
        self.excluded_folders = {'Hosted', 'System', 'Utilities'}
        self.session = requests.Session()
        
    def authenticate(self) -> bool:
        """
        Authenticate with ArcGIS Server and obtain a token.
        
        Returns:
            bool: True if authentication successful, False otherwise
        """
        auth_url = f"{self.server_url}/arcgis/admin/generateToken"
        
        auth_data = {
            'username': self.username,
            'password': self.password,
            'client': 'requestip',
            'f': 'json'
        }
        
        try:
            response = self.session.post(auth_url, data=auth_data, verify=False)
            response.raise_for_status()
            
            result = response.json()
            if 'token' in result:
                self.token = result['token']
                print("Authentication successful")
                return True
            else:
                print(f"Authentication failed: {result.get('error', {}).get('message', 'Unknown error')}")
                return False
                
        except requests.RequestException as e:
            print(f"Authentication request failed: {e}")
            return False
    
    def _make_request(self, endpoint: str, data: Dict = None, method: str = 'GET') -> Optional[Dict]:
        """
        Make an authenticated request to the ArcGIS Server REST API.
        
        Args:
            endpoint: API endpoint (relative to /arcgis/admin/)
            data: Request data for POST requests
            method: HTTP method ('GET' or 'POST')
            
        Returns:
            Response JSON as dict, or None if request failed
        """
        if not self.token:
            print("Not authenticated. Call authenticate() first.")
            return None
            
        url = f"{self.server_url}/arcgis/admin/{endpoint}"
        
        # Add token to request data
        request_data = data.copy() if data else {}
        request_data['token'] = self.token
        request_data['f'] = 'json'
        
        try:
            if method.upper() == 'GET':
                response = self.session.get(url, params=request_data, verify=False)
            else:
                response = self.session.post(url, data=request_data, verify=False)
                
            response.raise_for_status()
            result = response.json()
            
            if 'error' in result:
                print(f"API Error: {result['error'].get('message', 'Unknown error')}")
                return None
                
            return result
            
        except requests.RequestException as e:
            print(f"Request failed: {e}")
            return None
    
    def get_services(self) -> List[Dict]:
        """
        Get all services from the ArcGIS Server.
        
        Returns:
            List of service dictionaries with folder, name, and type information
        """
        services = []
        
        # Get services in root folder
        result = self._make_request('services')
        if result and 'services' in result:
            for service in result['services']:
                services.append({
                    'folder': '',
                    'name': service['serviceName'],
                    'type': service['type']
                })
        
        # Get services in subfolders
        if result and 'folders' in result:
            for folder in result['folders']:
                folder_result = self._make_request(f'services/{folder}')
                if folder_result and 'services' in folder_result:
                    for service in folder_result['services']:
                        services.append({
                            'folder': folder,
                            'name': service['serviceName'],
                            'type': service['type']
                        })
        
        return services
    
    def get_service_details(self, folder: str, service_name: str, service_type: str) -> Optional[Dict]:
        """
        Get detailed information about a specific service.
        
        Args:
            folder: Service folder (empty string for root)
            service_name: Service name
            service_type: Service type (e.g., 'MapServer')
            
        Returns:
            Service details dictionary
        """
        if folder:
            endpoint = f'services/{folder}/{service_name}.{service_type}'
        else:
            endpoint = f'services/{service_name}.{service_type}'
            
        return self._make_request(endpoint)
    
    def start_service(self, folder: str, service_name: str, service_type: str) -> bool:
        """Start a service."""
        if folder:
            endpoint = f'services/{folder}/{service_name}.{service_type}/start'
        else:
            endpoint = f'services/{service_name}.{service_type}/start'
            
        result = self._make_request(endpoint, method='POST')
        return result is not None and result.get('status') == 'success'
    
    def stop_service(self, folder: str, service_name: str, service_type: str) -> bool:
        """Stop a service."""
        if folder:
            endpoint = f'services/{folder}/{service_name}.{service_type}/stop'
        else:
            endpoint = f'services/{service_name}.{service_type}/stop'
            
        result = self._make_request(endpoint, method='POST')
        return result is not None and result.get('status') == 'success'
    
    def update_service_instances(self, folder: str, service_name: str, service_type: str, 
                               min_instances: int, max_instances: int) -> bool:
        """
        Update the min/max instances for a service.
        
        Args:
            folder: Service folder
            service_name: Service name
            service_type: Service type
            min_instances: Minimum instances
            max_instances: Maximum instances
            
        Returns:
            True if successful, False otherwise
        """
        # First get current service configuration
        service_details = self.get_service_details(folder, service_name, service_type)
        if not service_details:
            return False
        
        # Update instance settings
        service_details['minInstancesPerNode'] = min_instances
        service_details['maxInstancesPerNode'] = max_instances
        
        # Remove read-only properties that shouldn't be in the edit request
        readonly_props = ['status', 'configuredState', 'realTimeState', 'extensions']
        for prop in readonly_props:
            service_details.pop(prop, None)
        
        if folder:
            endpoint = f'services/{folder}/{service_name}.{service_type}/edit'
        else:
            endpoint = f'services/{service_name}.{service_type}/edit'
            
        edit_data = {
            'service': json.dumps(service_details)
        }
        
        result = self._make_request(endpoint, data=edit_data, method='POST')
        return result is not None and result.get('status') == 'success'
    
    def save_services_state(self, output_file: str) -> bool:
        """
        Save current service states and instance settings to CSV.
        
        Args:
            output_file: Output CSV file path
            
        Returns:
            True if successful, False otherwise
        """
        
        skipped_count = 0
        
        services = self.get_services()
        if not services:
            print("No services found or failed to retrieve services")
            return False
        
        try:
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['folder', 'service_name', 'service_type', 'configured_state', 
                            'min_instances', 'max_instances']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for service in services:
                    
                    # Skip services in excluded folders
                    if service['folder'] in self.excluded_folders:
                        service_path = f"{service['folder']}/{service['name']}.{service['type']}"
                        print(f"Skipping excluded service: {service_path}")
                        skipped_count += 1
                        continue
                    
                    details = self.get_service_details(service['folder'], service['name'], service['type'])
                    if details:
                        writer.writerow({
                            'folder': service['folder'],
                            'service_name': service['name'],
                            'service_type': service['type'],
                            'configured_state': details.get('configuredState', 'STOPPED'),
                            'min_instances': details.get('minInstancesPerNode', 1),
                            'max_instances': details.get('maxInstancesPerNode', 1)
                        })
                        print(f"Saved: {service['folder']}/{service['name']}.{service['type']}")
            
            print(f"Service states saved to {output_file}")
            return True
            
        except Exception as e:
            print(f"Failed to save services state: {e}")
            return False
    
    def stop_all_except_one(self, keep_service: str) -> bool:
        """
        Stop all services except one, and set that service's instances to 1/1.
        
        Args:
            keep_service: Name of the service to keep running
            
        Returns:
            True if successful, False otherwise
        """
        
        skipped_count = 0
        
        services = self.get_services()
        if not services:
            print("No services found or failed to retrieve services")
            return False
        
        kept_service_found = False
        
        for service in services:
            
            service_full_name = f"{service['name']}.{service['type']}"
            service_path = f"{service['folder']}/{service_full_name}" if service['folder'] else service_full_name

            # Skip services in excluded folders
            if service['folder'] in self.excluded_folders:
                service_path = f"{service['folder']}/{service['name']}.{service['type']}"
                print(f"Skipping excluded service: {service_path}")
                skipped_count += 1
                continue
            
            if service['name'] == keep_service:
                # This is the service to keep running
                print(f"Keeping service running: {service_path}")
                
                # Set instances to 1/1
                if self.update_service_instances(service['folder'], service['name'], 
                                               service['type'], 1, 1):
                    print(f"Updated instances to 1/1 for {service_path}")
                else:
                    print(f"Failed to update instances for {service_path}")
                
                # Ensure it's started
                if self.start_service(service['folder'], service['name'], service['type']):
                    print(f"Started service: {service_path}")
                else:
                    print(f"Failed to start service: {service_path}")
                
                kept_service_found = True
            else:
                # Stop this service
                if self.stop_service(service['folder'], service['name'], service['type']):
                    print(f"Stopped service: {service_path}")
                else:
                    print(f"Failed to stop service: {service_path}")
        
        if not kept_service_found:
            print(f"Warning: Service '{keep_service}' not found!")
            return False
        
        print("Service shutdown complete")
        return True
    
    def restore_services_state(self, input_file: str) -> bool:
        """
        Restore service states and instance settings from CSV.
        Excludes services in Hosted, System, and Utilities folders.
        
        Args:
            input_file: Input CSV file path
            
        Returns:
            True if successful, False otherwise
        """
        try:
            restored_count = 0
            skipped_count = 0
            
            with open(input_file, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                
                for row in reader:
                    folder = row['folder']
                    service_name = row['service_name']
                    service_type = row['service_type']
                    configured_state = row['configured_state']
                    min_instances = int(row['min_instances'])
                    max_instances = int(row['max_instances'])
                    
                    # Skip services in excluded folders
                    if folder in self.excluded_folders:
                        service_path = f"{folder}/{service_name}.{service_type}"
                        print(f"Skipping excluded service: {service_path}")
                        skipped_count += 1
                        continue
                    
                    service_path = f"{folder}/{service_name}.{service_type}" if folder else f"{service_name}.{service_type}"
                    
                    # Update instances
                    if self.update_service_instances(folder, service_name, service_type, 
                                                   min_instances, max_instances):
                        print(f"Updated instances for {service_path}: {min_instances}/{max_instances}")
                    else:
                        print(f"Failed to update instances for {service_path}")
                        continue
                    
                    # Set service state
                    if configured_state.upper() == 'STARTED':
                        if self.start_service(folder, service_name, service_type):
                            print(f"Started service: {service_path}")
                            restored_count += 1
                        else:
                            print(f"Failed to start service: {service_path}")
                    else:
                        if self.stop_service(folder, service_name, service_type):
                            print(f"Stopped service: {service_path}")
                            restored_count += 1
                        else:
                            print(f"Failed to stop service: {service_path}")
            
            print(f"Service restoration complete ({restored_count} services restored, {skipped_count} services skipped)")
            if skipped_count > 0:
                print(f"Excluded folders: {', '.join(self.excluded_folders)}")
            return True
            
        except FileNotFoundError:
            print(f"Input file '{input_file}' not found")
            return False
        except Exception as e:
            print(f"Failed to restore services state: {e}")
            return False


def main():
    parser = argparse.ArgumentParser(description='ArcGIS Server Service Manager')
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Common arguments for all commands
    def add_common_args(subparser):
        subparser.add_argument('--server', required=True, 
                             help='ArcGIS Server URL (e.g., https://server:6443)')
        subparser.add_argument('--username', required=True,
                             help='Primary Site Administrator username')
        subparser.add_argument('--password', required=True,
                             help='Primary Site Administrator password')
    
    # Save command
    save_parser = subparsers.add_parser('save', help='Save current service states to CSV')
    add_common_args(save_parser)
    save_parser.add_argument('--output', required=True,
                           help='Output CSV file path')
    
    # Stop command
    stop_parser = subparsers.add_parser('stop', help='Stop all services except one')
    add_common_args(stop_parser)
    stop_parser.add_argument('--keep-service', required=True,
                           help='Name of service to keep running')
    
    # Restore command
    restore_parser = subparsers.add_parser('restore', help='Restore service states from CSV')
    add_common_args(restore_parser)
    restore_parser.add_argument('--input', required=True,
                              help='Input CSV file path')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    # Disable SSL warnings (adjust as needed for your environment)
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    # Initialize manager and authenticate
    manager = ArcGISServerManager(args.server, args.username, args.password)
    
    if not manager.authenticate():
        print("Authentication failed")
        return 1
    
    # Execute command
    success = False
    
    if args.command == 'save':
        success = manager.save_services_state(args.output)
    elif args.command == 'stop':
        success = manager.stop_all_except_one(args.keep_service)
    elif args.command == 'restore':
        success = manager.restore_services_state(args.input)
    
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())